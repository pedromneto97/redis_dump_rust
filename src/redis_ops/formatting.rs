pub(crate) fn escape_quotes(s: &str) -> String {
    s.replace('"', "\\\"")
}

pub(crate) fn quoted(s: &str) -> String {
    format!("\"{}\"", escape_quotes(s))
}

pub(crate) fn append_expire_command(commands: &mut Vec<String>, key: &str, ttl: Option<i64>) {
    if let Some(ttl) = ttl {
        commands.push(format!("EXPIRE {} {}", quoted(key), ttl));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_quotes() {
        assert_eq!(escape_quotes("simple"), "simple");
        assert_eq!(escape_quotes("has\"quote"), "has\\\"quote");
        assert_eq!(escape_quotes("\"both\""), "\\\"both\\\"");
        assert_eq!(escape_quotes(""), "");
    }

    #[test]
    fn test_quoted() {
        assert_eq!(quoted("simple"), "\"simple\"");
        assert_eq!(quoted("has\"quote"), "\"has\\\"quote\"");
        assert_eq!(quoted(""), "\"\"");
    }

    #[test]
    fn test_append_expire_command_with_ttl() {
        let mut commands = Vec::new();
        append_expire_command(&mut commands, "mykey", Some(300));
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0], "EXPIRE \"mykey\" 300");
    }

    #[test]
    fn test_append_expire_command_without_ttl() {
        let mut commands = Vec::new();
        append_expire_command(&mut commands, "mykey", None);
        assert!(commands.is_empty());
    }
}
