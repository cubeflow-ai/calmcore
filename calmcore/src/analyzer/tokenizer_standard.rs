use super::*;

use unicode_segmentation::UnicodeSegmentation;

#[derive(Clone)]
pub struct StandardTokenizer;

impl StandardTokenizer {
    pub fn instance() -> Box<dyn Tokenizer + Send + Sync + 'static> {
        Box::new(StandardTokenizer {})
    }
}

impl Tokenizer for StandardTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        text.split_word_bounds().map(Token::new).collect()
    }
}

#[test]
fn test_standard_tokenizer() {
    let text = "你好rust，こんにちはろくでなしバガ456.3123 12.3℃ 3℃ abc@abc.com hello. .hello .123 !@#$%^&*()_" ;

    print!("{:?}", "你".len());

    let tz = StandardTokenizer {};

    for token in tz.tokenize(text) {
        println!("{:?}", token);
    }
}
