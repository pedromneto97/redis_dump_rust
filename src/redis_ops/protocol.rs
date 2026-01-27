use crate::config::OutputFormat;

pub(crate) fn format_resp_command(parts: &[&str]) -> String {
    let mut result = format!("*{}\r\n", parts.len());

    for part in parts {
        result.push_str(&format!("${}\r\n{part}\r\n", part.len()));
    }
    result
}

pub(crate) fn parse_redis_command(command: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current_part = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let chars: Vec<char> = command.trim().chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        if escape_next {
            current_part.push(ch);
            escape_next = false;
        } else if ch == '\\' {
            if in_quotes && i + 1 < chars.len() {
                let next_ch = chars[i + 1];
                if next_ch == '"' {
                    // This is an escaped quote inside quoted string
                    current_part.push('"');
                    i += 1; // Skip the next character (the escaped quote)
                } else {
                    // For non-quote escapes (like \x00), preserve the backslash
                    current_part.push(ch);
                }
            } else {
                escape_next = true;
            }
        } else if ch == '"' {
            if in_quotes {
                // Closing quote - add current part if it's not empty OR if we're closing an empty quoted string
                if !current_part.is_empty() || parts.is_empty() {
                    parts.push(current_part.clone());
                    current_part.clear();
                }
                in_quotes = false;
            } else {
                // Opening quote
                in_quotes = true;
                // If we have content before the quote, save it first
                if !current_part.is_empty() {
                    parts.push(current_part.clone());
                    current_part.clear();
                }
            }
        } else if ch.is_whitespace() && !in_quotes {
            if !current_part.is_empty() {
                parts.push(current_part.clone());
                current_part.clear();
            }
        } else {
            current_part.push(ch);
        }

        i += 1;
    }

    // Handle remaining content
    if !current_part.is_empty() || in_quotes {
        parts.push(current_part);
    }

    parts
}

pub(crate) fn format_command_output(command: &str, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Commands => format!("{command}\n"),
        OutputFormat::Resp => {
            let parts = parse_redis_command(command);
            let part_refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            format_resp_command(&part_refs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_resp_command_basic() {
        let parts = vec!["SET", "key", "value"];
        let resp = format_resp_command(&parts);
        assert!(resp.starts_with("*3\r\n"));
        assert!(resp.contains("$3\r\nSET\r\n"));
        assert!(resp.contains("$3\r\nkey\r\n"));
        assert!(resp.contains("$5\r\nvalue\r\n"));

        // Verify complete structure
        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(resp, expected);
    }

    #[test]
    fn test_format_resp_command_empty() {
        let parts: Vec<&str> = vec![];
        let resp = format_resp_command(&parts);
        assert_eq!(resp, "*0\r\n");
    }

    #[test]
    fn test_format_resp_command_single_part() {
        let parts = vec!["PING"];
        let resp = format_resp_command(&parts);
        assert_eq!(resp, "*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_format_resp_command_with_empty_string() {
        let parts = vec!["SET", "key", ""];
        let resp = format_resp_command(&parts);
        assert!(resp.contains("$0\r\n\r\n")); // Empty value
        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n";
        assert_eq!(resp, expected);
    }

    #[test]
    fn test_format_resp_command_with_special_chars() {
        let parts = vec!["SET", "key:with:colons", "value\nwith\nnewlines"];
        let resp = format_resp_command(&parts);
        assert!(resp.contains("key:with:colons"));
        assert!(resp.contains("value\nwith\nnewlines"));
    }

    #[test]
    fn test_parse_redis_command_simple() {
        let cmd = "SET key value";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_redis_command_quotes_and_whitespace() {
        let cmd = "SET \"key name\" \"value\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key name", "value"]);
    }

    #[test]
    fn test_parse_redis_command_mixed_quotes() {
        let cmd = "SET \"quoted key\" unquoted_value \"another quoted value\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(
            parsed,
            vec![
                "SET",
                "quoted key",
                "unquoted_value",
                "another quoted value"
            ]
        );
    }

    #[test]
    fn test_parse_redis_command_escaped_quotes() {
        let cmd = r#"SET key "value with \"escaped\" quotes""#;
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", r#"value with "escaped" quotes"#]);
    }

    #[test]
    fn test_parse_redis_command_empty() {
        let cmd = "";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, Vec::<String>::new());
    }

    #[test]
    fn test_parse_redis_command_whitespace_only() {
        let cmd = "   \t  \n  ";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, Vec::<String>::new());
    }

    #[test]
    fn test_parse_redis_command_single_word() {
        let cmd = "PING";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["PING"]);
    }

    #[test]
    fn test_parse_redis_command_multiple_spaces() {
        let cmd = "SET    key     value";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_redis_command_tabs_and_newlines() {
        let cmd = "SET\tkey\nvalue";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_redis_command_only_quotes() {
        let cmd = r#""""#;
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec![""]);
    }

    #[test]
    fn test_parse_redis_command_nested_quotes() {
        let cmd = r#"SET key "value with 'single quotes' inside""#;
        let parsed = parse_redis_command(cmd);
        assert_eq!(
            parsed,
            vec!["SET", "key", "value with 'single quotes' inside"]
        );
    }

    #[test]
    fn test_format_command_output_resp() {
        let cmd = "SET key value";
        let out = format_command_output(cmd, &OutputFormat::Resp);
        assert!(out.contains("*3\r\n"));
        assert!(out.contains("$3\r\nSET\r\n"));
        assert!(out.contains("$3\r\nkey\r\n"));
        assert!(out.contains("$5\r\nvalue\r\n"));
    }

    #[test]
    fn test_format_command_output_commands() {
        let cmd = "SET key value";
        let out = format_command_output(cmd, &OutputFormat::Commands);
        assert_eq!(out, "SET key value\n");
    }

    #[test]
    fn test_format_command_output_empty_command() {
        let cmd = "";
        let resp_out = format_command_output(cmd, &OutputFormat::Resp);
        let cmd_out = format_command_output(cmd, &OutputFormat::Commands);

        assert_eq!(resp_out, "*0\r\n");
        assert_eq!(cmd_out, "\n");
    }

    #[test]
    fn test_format_command_output_complex_command() {
        let cmd = r#"HMSET user:123 name "John Doe" email "john@example.com" age 30"#;
        let resp_out = format_command_output(cmd, &OutputFormat::Resp);
        let cmd_out = format_command_output(cmd, &OutputFormat::Commands);

        assert!(resp_out.starts_with("*8\r\n")); // 8 parts
        assert_eq!(cmd_out, format!("{}\n", cmd));
    }

    #[test]
    fn test_command_parsing_edge_cases() {
        // Test various Redis command formats
        let test_cases = vec![
            ("PING", vec!["PING"]),
            ("GET key", vec!["GET", "key"]),
            ("SET key value", vec!["SET", "key", "value"]),
            (
                "MSET key1 value1 key2 value2",
                vec!["MSET", "key1", "value1", "key2", "value2"],
            ),
            (
                "SADD myset member1 member2 member3",
                vec!["SADD", "myset", "member1", "member2", "member3"],
            ),
            ("EXPIRE key 3600", vec!["EXPIRE", "key", "3600"]),
        ];

        for (input, expected) in test_cases {
            let parsed = parse_redis_command(input);
            assert_eq!(parsed, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_resp_format_various_commands() {
        let test_cases = vec![
            ("PING", "*1\r\n$4\r\nPING\r\n"),
            ("GET key", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
            (
                "DEL key1 key2",
                "*3\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n",
            ),
        ];

        for (input, expected_resp) in test_cases {
            let output = format_command_output(input, &OutputFormat::Resp);
            assert_eq!(output, expected_resp, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_command_format_consistency() {
        let commands = vec![
            "SET key value",
            "GET key",
            "DEL key1 key2 key3",
            "HMSET hash field1 value1 field2 value2",
            "SADD set member1 member2",
            "ZADD zset 1.0 member1 2.0 member2",
            "LPUSH list item1 item2 item3",
        ];

        for cmd in commands {
            let parsed = parse_redis_command(cmd);
            let resp_output = format_command_output(cmd, &OutputFormat::Resp);
            let cmd_output = format_command_output(cmd, &OutputFormat::Commands);

            // Commands output should just add newline
            assert_eq!(cmd_output, format!("{}\n", cmd));

            // RESP output should be valid
            assert!(resp_output.starts_with(&format!("*{}\r\n", parsed.len())));

            // Each parsed part should appear in RESP output
            for part in &parsed {
                assert!(resp_output.contains(&format!("${}\r\n{}\r\n", part.len(), part)));
            }
        }
    }

    #[test]
    fn test_special_characters_in_commands() {
        let special_cases = vec![
            (
                "SET key \"value with spaces\"",
                vec!["SET", "key", "value with spaces"],
            ),
            (
                "SET key \"value\nwith\nnewlines\"",
                vec!["SET", "key", "value\nwith\nnewlines"],
            ),
            (
                "SET \"key:with:colons\" value",
                vec!["SET", "key:with:colons", "value"],
            ),
            (
                "SET key \"value,with,commas\"",
                vec!["SET", "key", "value,with,commas"],
            ),
            (
                "SET key \"value;with;semicolons\"",
                vec!["SET", "key", "value;with;semicolons"],
            ),
        ];

        for (input, expected) in special_cases {
            let parsed = parse_redis_command(input);
            assert_eq!(parsed, expected, "Failed for input: {}", input);

            // Test that formatting works correctly
            let resp_out = format_command_output(input, &OutputFormat::Resp);
            let cmd_out = format_command_output(input, &OutputFormat::Commands);

            assert!(!resp_out.is_empty());
            assert!(!cmd_out.is_empty());
        }
    }

    #[test]
    fn test_unicode_characters() {
        let cmd = "SET key \"测试数据\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "key", "测试数据"]);

        let resp_out = format_command_output(cmd, &OutputFormat::Resp);
        assert!(resp_out.contains("测试数据"));
    }

    #[test]
    fn test_binary_safe_data() {
        // Test with data that could contain null bytes or binary data (as string representation)
        let cmd = "SET binary_key \"\\x00\\x01\\x02\\x03\"";
        let parsed = parse_redis_command(cmd);
        assert_eq!(parsed, vec!["SET", "binary_key", "\\x00\\x01\\x02\\x03"]);
    }

    #[test]
    fn test_resp_format_with_special_lengths() {
        // Test RESP formatting with various string lengths
        let test_cases = vec![
            (vec![], "*0\r\n"),               // Empty array
            (vec![""], "*1\r\n$0\r\n\r\n"),   // Single empty string
            (vec!["a"], "*1\r\n$1\r\na\r\n"), // Single character
            (
                vec!["hello", "world"],
                "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            ), // Multiple strings
        ];

        for (input, expected) in test_cases {
            let resp = format_resp_command(&input);
            assert_eq!(resp, expected, "Failed for input: {:?}", input);
        }
    }

    #[test]
    fn test_parse_command_error_handling() {
        // Test parsing commands with various edge cases and malformed input
        let test_cases = vec![
            // Unmatched quotes should still parse gracefully
            (
                "SET key \"unmatched quote",
                vec!["SET", "key", "unmatched quote"],
            ),
            // Multiple consecutive quotes (the parser handles this by treating them as opening/closing)
            ("SET \"\"\"key\"\"\" value", vec!["SET", "key", "value"]),
            // Quote at the end starts a new quoted section that's empty
            ("SET key value\"", vec!["SET", "key", "value", ""]),
            // Mixed whitespace characters (all treated as separators)
            ("  SET\r\n\tkey\x0Bvalue  ", vec!["SET", "key", "value"]),
        ];

        for (input, expected) in test_cases {
            let parsed = parse_redis_command(input);
            assert_eq!(parsed, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_format_output_consistency() {
        // Ensure that parsing and formatting are consistent
        let original_commands = vec![
            "SET key value",
            "HMSET hash field1 value1 field2 value2",
            "SADD set member1 member2",
            "LPUSH list item1 item2",
        ];

        for cmd in original_commands {
            let parsed = parse_redis_command(cmd);
            let resp_output = format_command_output(cmd, &OutputFormat::Resp);
            let cmd_output = format_command_output(cmd, &OutputFormat::Commands);

            // Commands output should preserve the original with newline
            assert_eq!(cmd_output, format!("{}\n", cmd));

            // RESP should be properly formatted
            assert!(resp_output.starts_with(&format!("*{}\r\n", parsed.len())));

            // Round-trip test: parsed parts should match original intent
            assert!(
                !parsed.is_empty(),
                "Parsing should not return empty result for: {}",
                cmd
            );
        }
    }
}
