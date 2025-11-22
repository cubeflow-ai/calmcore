use super::*;

#[derive(Clone)]
pub struct WhitespaceTokenizer;

impl WhitespaceTokenizer {
    pub fn instance() -> Box<dyn Tokenizer + Send + Sync + 'static> {
        Box::new(WhitespaceTokenizer {})
    }
}

impl Tokenizer for WhitespaceTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        text.split_whitespace().map(Token::new).collect()
    }
}

#[test]
fn test_tokenizer() {
    let text = "你好rust，こんにちはろくでなしバガ456.3123 12.3℃ 3℃ abc@abc.com hello. .hello .123 !@#$%^&*()_" ;

    print!("{:?}", "你".len());

    let tz = WhitespaceTokenizer {};

    for token in tz.tokenize(text) {
        println!("{:?}", token);
    }
}
