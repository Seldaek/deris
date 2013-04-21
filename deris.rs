extern mod std;
use std::net::tcp;
use std::net::ip;
use std::uv;
use std::arc::RWARC;
use core::hashmap::linear::LinearMap; // core::hashmap::HashMap in 0.7

fn main() {
    io::println("Started");
    let iotask = &uv::global_loop::get();
    let port = 6380;

    let mut data = ~LinearMap::new::<~[u8], ~[u8]>();
    let dataARC = RWARC(data);

    let listen_res = do tcp::listen(ip::v4::parse_addr("127.0.0.1"), port, 1000, iotask,
        |_kill_ch| {
            // pass the kill_ch to your main loop or wherever you want
            // to be able to externally kill the server from
            io::println(fmt!("Server is listening on port %u", port));
        }
    ) |new_conn, kill_ch| {
        io::println("New client");

        let (port, channel) = comm::stream::<Option<tcp::TcpErrData>>();
        let localARC = dataARC.clone();

        do task::spawn_supervised {
            let accept_result = tcp::accept(new_conn);
            match accept_result {
                Err(accept_error) => {
                    io::stderr().write_line("Failed to accept connection");
                    channel.send(Some(accept_error));
                },
                Ok(sock) => {
                    let peer_addr = &sock.get_peer_addr();
                    io::println(fmt!("Client connected: %s on port %u", ip::format_addr(peer_addr), ip::get_port(peer_addr)));
                    channel.send(None);

                    let sockbuf = tcp::socket_buf(sock);
                    let crlf = "\r\n".to_bytes();
                    loop {
                        let parse_res = parse_args(sockbuf);
                        match parse_res {
                            Err(msg) => {
                                sockbuf.write_str(msg);
                                sockbuf.write(crlf);
                            },
                            Ok(args) => {
                                let mut response = cmd_dispatcher(&localARC, args);
                                vec::push_all(&mut response, crlf);
                                sockbuf.write(response);
                            }
                        }
                    }
                }
            }
        };

        match port.recv() {
          // shut down listen()
          Some(err_data) => kill_ch.send(Some(err_data)),
          // wait for next connection
          None => ()
        }
    };

    if listen_res.is_err() {
        io::stderr().write_line(fmt!("Failed to bind address: %?", listen_res.get_err()));
    }
}

fn parse_args(buf: tcp::TcpSocketBuf) -> Result<~[~[u8]], ~str> {
    let first_char = buf.read_char();
    let has_arg_count = first_char == '*';
    let mut args = ~[];

    if has_arg_count {
        let mut arg_count;
        match uint::from_str(buf.read_until('\r', false)) {
            Some(count) => {
                arg_count = count;
            },
            None => {
                return Err(~"Could not parse argument count as uint");
            }
        }

        // discard \n
        buf.read_byte();

        while arg_count > 0 {
            let chr = buf.read_char();
            let has_arg_len = chr == '$';
            if !has_arg_len {
                return Err(fmt!("No argument length found, expected $n, got %?", chr));
            }
            let arg_len;
            match uint::from_str(buf.read_until('\r', false)) {
                Some(count) => {
                    arg_len = count;
                },
                None => {
                    return Err(~"Could not parse argument length as uint");
                }
            }
            // discard \n
            buf.read_byte();

            args.push(buf.read_bytes(arg_len));
            arg_count -= 1;

            // discard \r\n
            buf.read_bytes(2);
        }
    } else {
        let input = str::append(str::from_char(first_char), buf.read_until('\r', false));
        // discard \n
        buf.read_char();

        for str::each_split_char_nonempty(input, ' ') |word| {
            args.push(word.to_bytes());
        }
    }

    Ok(args)
}

fn cmd_dispatcher(arc: &RWARC<~LinearMap<~[u8], ~[u8]>>, mut args: ~[~[u8]]) -> ~[u8] {
    let command = str::from_bytes(args[0]).to_lower();

    //io::println(fmt!("%s: %?", command, args));

    let mut output = ~[];
    if command == ~"get" {
        do arc.read() |data| {
            let res = data.find(&args[1]);
            if res.is_some() {
                output = str::concat(~[~"$", res.unwrap().len().to_str(), ~"\r\n"]).to_bytes();
                vec::push_all(&mut output, *res.unwrap());
            } else {
                output = "$-1".to_bytes();
            }
        }
    } else if command == ~"set" {
        do arc.write() |data| {
            args.truncate(3);
            let val = args.pop();
            let key = args.pop();
            data.insert(key, val);

            output = "+OK".to_bytes();
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
                        output = "-ERR value is not an integer or out of range".to_bytes();
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
                    output = "-ERR incrby argument is not an integer or out of range".to_bytes();
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
                let new_val = (cur_val + incr_by).to_str().to_bytes();
                output = ":".to_bytes();
                vec::push_all(&mut output, new_val);
                data.insert(key, new_val);
            }
        }
    } else {
        output = fmt!("-ERR unknown command '%s'", command).to_bytes();
    }

    output
}
