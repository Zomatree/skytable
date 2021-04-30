/*
 * Created on Tue Aug 04 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{wrap_pyfunction, create_exception};

create_exception!(skytable, IncompleteResponse, pyo3::exceptions::PyException);
create_exception!(skytable, InvalidResponse, pyo3::exceptions::PyException);

#[pyfunction]
pub fn parse(buffer: &PyBytes) -> PyResult<Vec<Vec<(char, String)>>> {
    let buf = buffer.as_bytes();
    if buf.len()< 6 {
        // A packet that has less than 6 characters? Nonsense!
        return Err(IncompleteResponse::new_err("response is too small"));
    }
    /*
    We first get the metaframe, which looks something like:
    ```
    #<numchars_in_next_line>\n
    !<num_of_datagroups>\n
    ```
    */
    let mut pos = 0;
    if buf[pos] != b'#' {
        return Err(InvalidResponse::new_err("missing first line"));
    } else {
        pos += 1;
    }
    let next_line = match read_line_and_return_next_line(&mut pos, &buf) {
        Some(line) => line,
        None => {
            // This is incomplete
            return Err(IncompleteResponse::new_err("missing lines"));
        }
    };
    pos += 1; // Skip LF
              // Find out the number of actions that we have to do
    let mut action_size = 0usize;
    if next_line[0] == b'*' {
        let mut line_iter = next_line.into_iter().skip(1).peekable();
        while let Some(dig) = line_iter.next() {
            let curdig: usize = match dig.checked_sub(48) {
                Some(dig) => {
                    if dig > 9 {
                        return Err(InvalidResponse::new_err("invalid character"));
                    } else {
                        dig.into()
                    }
                }
                None => return Err(InvalidResponse::new_err("invalid character")),
            };
            action_size = (action_size * 10) + curdig;
        }
    // This line gives us the number of actions
    } else {
        return Err(InvalidResponse::new_err("..."));
    }
    let mut items: Vec<Vec<(char, String)>> = Vec::with_capacity(action_size);
    while pos < buf.len() && items.len() <= action_size {
        match buf[pos] {
            b'#' => {
                pos += 1; // Skip '#'
                let next_line = match read_line_and_return_next_line(&mut pos, &buf) {
                    Some(line) => line,
                    None => {
                        // This is incomplete
                        return Err(IncompleteResponse::new_err("missing lines"));
                    }
                }; // Now we have the current line
                pos += 1; // Skip the newline
                          // Move the cursor ahead by the number of bytes that we just read
                          // Let us check the current char
                match next_line[0] {
                    b'&' => {
                        // This is an array
                        // Now let us parse the array size
                        let mut current_array_size = 0usize;
                        let mut linepos = 1; // Skip the '&' character
                        while linepos < next_line.len() {
                            let curdg: usize = match next_line[linepos].checked_sub(48) {
                                Some(dig) => {
                                    if dig > 9 {
                                        // If `dig` is greater than 9, then the current
                                        // UTF-8 char isn't a number
                                        return Err(InvalidResponse::new_err("invalid character"));
                                    } else {
                                        dig.into()
                                    }
                                }
                                None => return Err(InvalidResponse::new_err("invalid character")),
                            };
                            current_array_size = (current_array_size * 10) + curdg; // Increment the size
                            linepos += 1; // Move the position ahead, since we just read another char
                        }
                        // Now we know the array size, good!
                        let mut actiongroup: Vec<(char, String)> = Vec::with_capacity(current_array_size);
                        // Let's loop over to get the elements till the size of this array
                        while pos < buf.len() && actiongroup.len() < current_array_size {
                            let mut element_size = 0usize;
                            let datatype = buf[pos];
                            pos += 1; // We've got the tsymbol above, so skip it
                            while pos < buf.len() && buf[pos] != b'\n' {
                                let curdig: usize = match buf[pos].checked_sub(48) {
                                    Some(dig) => {
                                        if dig > 9 {
                                            // If `dig` is greater than 9, then the current
                                            // UTF-8 char isn't a number
                                            return Err(InvalidResponse::new_err("invalid character"));
                                        } else {
                                            dig.into()
                                        }
                                    }
                                    None => return Err(InvalidResponse::new_err("invalid character")),
                                };
                                element_size = (element_size * 10) + curdig; // Increment the size
                                pos += 1; // Move the position ahead, since we just read another char
                            }
                            pos += 1;
                            // We now know the item size
                            let mut value = String::with_capacity(element_size);
                            let extracted = match buf.get(pos..pos + element_size) {
                                Some(s) => s,
                                None => return Err(IncompleteResponse::new_err("...")),
                            };
                            pos += element_size; // Move the position ahead
                            value.push_str(&String::from_utf8_lossy(extracted));
                            pos += 1; // Skip the newline
                            actiongroup.push((char::from_u32(datatype.into()).unwrap(), value));
                        }
                        items.push(actiongroup);
                    }
                    _ => return Err(InvalidResponse::new_err("invalid character")),
                }
                continue;
            }
            _ => {
                // Since the variant '#' would does all the array
                // parsing business, we should never reach here unless
                // the packet is invalid
                return Err(InvalidResponse::new_err("invalid response"));
            }
        }
    }
    if buf.get(pos).is_none() {
        // Either more data was sent or some data was missing
        if items.len() == action_size {
            if items.len() == 1 {
                Ok(items)
            } else {
                // The CLI does not support batch queries
                unimplemented!();
            }
        } else {
            Err(IncompleteResponse::new_err("..."))
        }
    } else {
        Err(InvalidResponse::new_err("invalid character"))
    }
}
/// Read a size line and return the following line
///
/// This reads a line that begins with the number, i.e make sure that
/// the **`#` character is skipped**
///
fn read_line_and_return_next_line<'a>(pos: &mut usize, buf: &'a [u8]) -> Option<&'a [u8]> {
    let mut next_line_size = 0usize;
    while pos < &mut buf.len() && buf[*pos] != b'\n' {
        // 48 is the UTF-8 code for '0'
        let curdig: usize = match buf[*pos].checked_sub(48) {
            Some(dig) => {
                if dig > 9 {
                    // If `dig` is greater than 9, then the current
                    // UTF-8 char isn't a number
                    return None;
                } else {
                    dig.into()
                }
            }
            None => return None,
        };
        next_line_size = (next_line_size * 10) + curdig; // Increment the size
        *pos += 1; // Move the position ahead, since we just read another char
    }
    *pos += 1; // Skip the newline
               // We now know the size of the next line
    let next_line = match buf.get(*pos..*pos + next_line_size) {
        Some(line) => line,
        None => {
            // This is incomplete
            return None;
        }
    }; // Now we have the current line
       // Move the cursor ahead by the number of bytes that we just read
    *pos += next_line_size;
    Some(next_line)
}

#[pymodule]
fn skytable(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(parse))?;

    Ok(())
}
