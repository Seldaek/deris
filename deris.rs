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

    let mut data = ~LinearMap::new::<~str, ~str>();
    let arc = RWARC(data);

    let listen_res = do tcp::listen(ip::v4::parse_addr("127.0.0.1"), port, 1000, iotask,
        |_kill_ch| {
            // pass the kill_ch to your main loop or wherever you want
            // to be able to externally kill the server from
            io::println(fmt!("Server is listening on port %u", port));
        }
    ) |new_conn, kill_ch| {
        io::println("New client");

        let (port, channel) = comm::stream::<option::Option<tcp::TcpErrData>>();
        let localARC = arc.clone();
        do task::spawn {
            let accept_result = tcp::accept(new_conn);
            match accept_result {
                Err(accept_error) => {
                    io::println("Failed!");
                    channel.send(Some(accept_error));
                    // fail?
                },
                Ok(sock) => {
                    io::println("Accepted!");
                    channel.send(None);
                    // do work here
                    let s = ~"Hello\n";
                    do str::as_bytes(&s) |&bytes| {
                        sock.write(bytes);
                    }

                    let sockbuf = tcp::socket_buf(sock);
                    loop {
                        let command = sockbuf.read_line();
                        let s = cmd_dispatcher(localARC.clone(), command);
                        sockbuf.write_line(s);
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
        io::println(fmt!("Failed to bind address: %?", listen_res.get_err()));
    }
}

fn cmd_dispatcher(arc: RWARC<~LinearMap<~str, ~str>>, input: &str) -> ~str {
    let mut args = ~[];

    for str::each_split_char_nonempty(input, ' ') |word| {
        args.push(word.to_str());
    }

    // remove trailing \r
    str::pop_char(&mut args[args.len()-1]);

    // grab command
    let command = args.shift();

    io::println(fmt!("%s: %?", command, args));

    let mut output = ~"-ERR Command not found";

    if command == ~"get" {
        do arc.read() |data| {
            let res = data.find(&args[0]);
            if res.is_some() {
                output = res.unwrap().to_str();
            } else {
                output = ~"$-1";
            }
        }
    } else if command == ~"set" {
        do arc.write() |data| {
            data.insert(args[0].to_str(), args[1].to_str());

            output = ~"+OK";
        }
    }

    return output;
}
