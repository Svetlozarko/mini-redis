use std::sync::Arc;
use std::io::{self, Write};

use rust_redis::db::Database; // Adjust this import path based on your project structure
use rust_redis::commands::handle_command; // Your command handler function

#[tokio::main]
async fn main() {
    let db = Database::new();

    println!("Rust Redis Manual Test CLI");
    println!("Type commands like SET key value, GET key, DEL key, EXPIRE key seconds, etc.");
    println!("Type QUIT or EXIT to stop.");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            println!("Failed to read line");
            continue;
        }

        let input = input.trim();
        if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") {
            println!("Exiting.");
            break;
        }

        let response = handle_command(input, &db);
        print!("{}", response);
    }
}

