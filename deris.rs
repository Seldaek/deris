extern mod extra;

use std::cell::Cell;
use std::rt::io::net::tcp::{TcpListener, TcpStream};
use std::rt::io::net::ip::Ipv4;
use std::rt::io::{Listener, Reader, ReaderUtil, Writer};
use extra::arc::RWArc;
use std::hashmap::HashMap;
use std::{uint, int, str};

fn main() {
    println("Started");

    let data = ~HashMap::new::<~[u8], ~[u8]>();
    let data_arc = RWArc::new(data);

    let port = 6380;
    let mut listener = TcpListener::bind(Ipv4(127, 0, 0, 1, port)).expect("Unable to bind to 127.0.0.1:6380");
    println(fmt!("Server is listening on %s", listener.socket_name().expect("").to_str()));

    loop {
        let stream = Cell::new(listener.accept());

        let local_arc = data_arc.clone();

        do std::task::spawn_supervised {
            let mut stream = stream.take().unwrap();
            println(fmt!("Client connected: %s", stream.peer_name().expect("").to_str()));

            let crlf = bytes!("\r\n");
            loop {
                let parse_res = parse_args(&mut stream);
                let mut response;
                match parse_res {
                    Err(msg) => {
                        response = msg.as_bytes().to_owned();
                    },
                    Ok(args) => {
                        response = cmd_dispatcher(&local_arc, args);
                    }
                }
                response.push_all(crlf);
                stream.write(response);
            }
        }
    }
}

fn read_byte(stream: &mut TcpStream) -> u8 {
    stream.read_byte().expect("")
}

fn read_bytes(stream: &mut TcpStream, len: uint) -> ~[u8] {
    let mut buffer = std::vec::with_capacity::<u8>(len);

    for i in range(0, len) {
        buffer[i] = stream.read_byte().expect("");
    }

    buffer
}

fn read_until(stream: &mut TcpStream, until: u8) -> ~[u8] {
    let mut buffer: ~[u8] = ~[];

    loop {
        let byte = stream.read_byte().expect("");
        if byte == until {
            break
        }
        buffer.push(byte);
    }

    buffer
}

static CR: u8 = '\r' as u8;
static STAR: u8 = '*' as u8;
static DOLLAR: u8 = '$' as u8;

fn parse_args(stream: &mut TcpStream) -> Result<~[~[u8]], ~str> {
    let first_byte = read_byte(stream);
    let mut args = ~[];

    if first_byte == STAR {
        let mut arg_count;
        match uint::parse_bytes(read_until(stream, CR), 10) {
            Some(count) => {
                arg_count = count;
            },
            None => {
                return Err(~"Could not parse argument count as uint");
            }
        }

        // discard \n
        read_byte(stream);

        for _ in range(0, arg_count) {
            let byte = read_byte(stream);
            if byte != DOLLAR {
                return Err(fmt!("No argument length found, expected $, got %?", byte));
            }
            let arg_len;
            match uint::parse_bytes(read_until(stream, CR), 10) {
                Some(count) => {
                    arg_len = count;
                },
                None => {
                    return Err(~"Could not parse argument length as uint");
                }
            }
            // discard \n
            read_byte(stream);

            args.push(stream.read_bytes(arg_len));

            // discard \r\n
            stream.read_bytes(2);
        }
    } else {
        let input = str::from_byte(first_byte);
        let input = input.append(str::from_bytes(read_until(stream, CR)));

        // discard \n
        stream.read_byte();

        let words: ~[&str] = input.split_iter(' ').collect();
        args = words.map(|word| {
            word.as_bytes().to_owned()
        })
    }

    Ok(args)
}

fn cmd_dispatcher(arc: &RWArc<~HashMap<~[u8], ~[u8]>>, mut args: ~[~[u8]]) -> ~[u8] {
    let command = args[0].to_ascii().to_lower().into_str().to_owned();

    //println(fmt!("%s: %?", command, args));

    let mut output = ~[];
    if command == ~"get" {
        do arc.read() |data| {
            let res = data.find(&args[1]);
            if res.is_some() {
                let result = [~"$", res.unwrap().len().to_str(), ~"\r\n"].concat();
                output = result.as_bytes().to_owned();
                output.push_all(*res.unwrap());
            } else {
                output = bytes!("$-1").to_owned();
            }
        }
    } else if command == ~"set" {
        if (args.len() < 3) {
            output = bytes!("-ERR wrong number of arguments for 'set' command").to_owned();
        } else {
            do arc.write() |data| {
                args.truncate(3);
                let val = args.pop();
                let key = args.pop();
                data.insert(key, val);

                output = bytes!("+OK").to_owned();
            }
        }
    } else if command == ~"incr" || command == ~"decr" || command == ~"incrby" || command == ~"decrby" {
        let mut cur_val = 0;

        do arc.read() |data| {
            let res = data.find(&args[1]);

            if res.is_some() {
                match int::parse_bytes(*res.unwrap(), 10) {
                    Some(val) => {
                        cur_val = val;
                    },
                    None => {
                        output = bytes!("-ERR value is not an integer or out of range").to_owned();
                    }
                }
            }
        }

        let mut incr_by = 1;
        if command == ~"incrby" || command == ~"decrby" {
            args.truncate(3);
            match int::parse_bytes(args.pop(), 10) {
                Some(val) => {
                    incr_by = val;
                },
                None => {
                    output = bytes!("-ERR incrby argument is not an integer or out of range").to_owned();
                }
            }
        }
        if command == ~"decr" || command == ~"decrby" {
            incr_by = -incr_by;
        }

        if output.len() == 0 {
            do arc.write() |data| {
                args.truncate(2);
                let key = args.pop();
                let new_val = (cur_val + incr_by).to_str();
                let new_val = new_val.as_bytes().to_owned();
                output = bytes!(":").to_owned();
                output.push_all(new_val);
                data.insert(key, new_val);
            }
        }
    } else {
        let result = fmt!("-ERR unknown command '%s'", command);
        output = result.as_bytes().to_owned();
    }

    output
}
