pub mod base58;

pub trait GenerateShortcode {
    fn generate_shortcode(&self) -> String;
}
