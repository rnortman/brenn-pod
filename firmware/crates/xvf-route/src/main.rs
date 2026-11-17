//! `xvf-route`: read or write the output routing of the XVF3800 on a reSpeaker
//! Flex running Seeed's six-channel USB audio image (`2886:0022`).
//!
//! ```text
//! xvf-route show --card <NAME>
//! xvf-route set --card <NAME> --op-all <b0,b1,...,b11>
//! ```
//!
//! `show` reads `AUDIO_MGR_OP_ALL` and the six `PK` registers and writes
//! nothing. `set` reads `AUDIO_MGR_OP_ALL`, writes the twelve bytes given, and
//! reads it back. The write is volatile: a power cycle undoes it.
//!
//! One JSON object on stdout; prose on stderr. Exit 0 on success, 1 on a
//! failure, 2 for a command line that does not parse.

use std::path::Path;
use std::process::ExitCode;

use serde_json::{Value, json};
use xvf_route::{
    OpAll, bytes, flex_device, open_writer, read_op_all, read_slot, set_command, write_op_all,
};
use xvf3800_ctrl::{AUDIO_MGR_OP_ALL_READ_LEN, OP_ALL_SLOTS};
use xvf3800_usb::{DEV_BUS_USB, open_reader};

const USAGE: &str = "usage: xvf-route show --card <NAME>\n       \
                     xvf-route set --card <NAME> --op-all <b0,b1,...,b11>";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Show { card: String },
    Set { card: String, op_all: OpAll },
}

/// Parse the arguments after the program name. Every flag is required, none
/// may repeat, and nothing else is accepted.
fn parse(args: &[String]) -> Result<Command, String> {
    let (verb, rest) = args.split_first().ok_or("no command given")?;
    let mut card = None;
    let mut op_all = None;
    let mut it = rest.iter();
    while let Some(flag) = it.next() {
        let slot = match flag.as_str() {
            "--card" => &mut card,
            "--op-all" if verb.as_str() == "set" => &mut op_all,
            other => return Err(format!("unexpected argument {other:?}")),
        };
        let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        if slot.replace(value.clone()).is_some() {
            return Err(format!("{flag} given twice"));
        }
    }
    let card = card.ok_or("--card is required")?;
    if card.is_empty() {
        return Err("--card is empty".into());
    }
    match verb.as_str() {
        "show" => Ok(Command::Show { card }),
        "set" => {
            let op_all = op_all.ok_or("--op-all is required")?;
            Ok(Command::Set {
                card,
                op_all: parse_op_all(&op_all)?,
            })
        }
        other => Err(format!("unknown command {other:?}")),
    }
}

/// Exactly twelve comma-separated decimal bytes.
fn parse_op_all(text: &str) -> Result<OpAll, String> {
    let values = text
        .split(',')
        .map(|v| {
            v.parse::<u8>()
                .map_err(|_| format!("--op-all: {v:?} is not a decimal byte"))
        })
        .collect::<Result<Vec<u8>, String>>()?;
    values.as_slice().try_into().map_err(|_| {
        format!(
            "--op-all: {} values; {AUDIO_MGR_OP_ALL_READ_LEN} are required",
            values.len()
        )
    })
}

fn show(card: &str) -> Result<Value, String> {
    let device = flex_device(card)?;
    eprintln!("{device}");
    let mut reader = open_reader(&device, Path::new(DEV_BUS_USB)).map_err(|e| e.to_string())?;
    let op_all = read_op_all(&mut reader).map_err(|e| e.to_string())?;
    let mut slots = serde_json::Map::new();
    for slot in &OP_ALL_SLOTS {
        let pair = read_slot(&mut reader, slot).map_err(|e| e.to_string())?;
        slots.insert(slot.name.to_owned(), json!(pair));
    }
    Ok(json!({
        "outcome": "read",
        "device": device.to_string(),
        "audio_mgr_op_all": op_all,
        "slots": slots,
    }))
}

fn set(card: &str, op_all: &OpAll) -> Result<Value, String> {
    let device = flex_device(card)?;
    eprintln!("{device}");
    let mut writer = open_writer(&device, Path::new(DEV_BUS_USB)).map_err(|e| e.to_string())?;
    let before = read_op_all(&mut writer).map_err(|e| e.to_string())?;
    eprintln!("before: {}", bytes(&before));
    eprintln!("to restore: {}", set_command(card, &before));
    match write_op_all(&mut writer, op_all) {
        Ok(written) => {
            eprintln!("after: {}", bytes(op_all));
            Ok(json!({
                "outcome": "written",
                "device": device.to_string(),
                "before": before,
                "after": op_all,
                "write_error": written.write_error,
            }))
        }
        Err(e) => Err(format!("{e}; it held {} before", bytes(&before))),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match parse(&args) {
        Ok(c) => c,
        Err(why) => {
            eprintln!("xvf-route: {why}\n{USAGE}");
            println!("{}", json!({"outcome": "usage", "error": why}));
            return ExitCode::from(2);
        }
    };
    let result = match &command {
        Command::Show { card } => show(card),
        Command::Set { card, op_all } => set(card, op_all),
    };
    match result {
        Ok(value) => {
            println!("{value}");
            ExitCode::SUCCESS
        }
        Err(why) => {
            eprintln!("xvf-route: {why}");
            println!("{}", json!({"outcome": "failed", "error": why}));
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(str::to_owned).collect()
    }

    #[test]
    fn show_takes_a_card_and_nothing_else() {
        assert_eq!(
            parse(&args("show --card Flex")),
            Ok(Command::Show {
                card: "Flex".into()
            })
        );
        for bad in [
            "",
            "show",
            "show --card",
            "show --card A --card B",
            "show --card A --op-all 8,0,1,0,1,2,7,3,1,1,1,3",
            "show --card A extra",
            "list --card A",
        ] {
            assert!(parse(&args(bad)).is_err(), "{bad:?}");
        }
        assert!(parse(&["show".into(), "--card".into(), String::new()]).is_err());
    }

    #[test]
    fn set_takes_a_card_and_exactly_twelve_bytes() {
        assert_eq!(
            parse(&args("set --op-all 8,0,12,0,1,2,7,3,1,1,1,3 --card Flex")),
            Ok(Command::Set {
                card: "Flex".into(),
                op_all: [8, 0, 12, 0, 1, 2, 7, 3, 1, 1, 1, 3]
            })
        );
        for bad in [
            "set --card Flex",
            "set --op-all 8,0,1,0,1,2,7,3,1,1,1,3",
            "set --card Flex --op-all 8,0,1,0,1,2,7,3,1,1,1",
            "set --card Flex --op-all 8,0,1,0,1,2,7,3,1,1,1,3,0",
            "set --card Flex --op-all 8,0,1,0,1,2,7,3,1,1,1,256",
            "set --card Flex --op-all 8,0,1,0,1,2,7,3,1,1,1,-1",
            "set --card Flex --op-all 8,0,1,0,1,2,7,3,1,1,1,",
            "set --card Flex --op-all 0x8,0,1,0,1,2,7,3,1,1,1,3",
            "set --card Flex --op-all 8,0,1,0,1,2,7,3,1,1,1,3 --op-all 8,0,1,0,1,2,7,3,1,1,1,3",
        ] {
            assert!(parse(&args(bad)).is_err(), "{bad:?}");
        }
    }
}
