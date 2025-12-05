//! Simple analyzer for testing full-text index without external dependencies

/// Token produced by tokenization
#[derive(Debug, Clone)]
pub struct Token {
    /// Token text
    pub name: String,
    /// Position in the document
    pub index: usize,
}

impl Token {
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
        }
    }
}

/// Simple analyzer that splits on whitespace and lowercases
#[derive(Debug, Clone)]
pub struct SimpleAnalyzer {
    lowercase: bool,
}

impl SimpleAnalyzer {
    pub fn new() -> Self {
        Self { lowercase: true }
    }

    pub fn with_lowercase(lowercase: bool) -> Self {
        Self { lowercase }
    }

    /// Analyze text for indexing
    pub fn analyzer_index(&self, text: &str) -> Vec<Token> {
        let text = if self.lowercase {
            text.to_lowercase()
        } else {
            text.to_string()
        };

        text.split_whitespace()
            .enumerate()
            .map(|(idx, word)| Token::new(word, idx))
            .collect()
    }

    /// Analyze text for querying (same as indexing for simple analyzer)
    pub fn analyzer_query(&self, text: &str) -> Vec<Token> {
        self.analyzer_index(text)
    }
}

impl Default for SimpleAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_analyzer() {
        let analyzer = SimpleAnalyzer::new();
        let tokens = analyzer.analyzer_index("The Quick BROWN fox");

        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].name, "the");
        assert_eq!(tokens[1].name, "quick");
        assert_eq!(tokens[2].name, "brown");
        assert_eq!(tokens[3].name, "fox");

        assert_eq!(tokens[0].index, 0);
        assert_eq!(tokens[1].index, 1);
        assert_eq!(tokens[2].index, 2);
        assert_eq!(tokens[3].index, 3);
    }

    #[test]
    fn test_no_lowercase() {
        let analyzer = SimpleAnalyzer::with_lowercase(false);
        let tokens = analyzer.analyzer_index("The Quick");

        assert_eq!(tokens[0].name, "The");
        assert_eq!(tokens[1].name, "Quick");
    }
}
