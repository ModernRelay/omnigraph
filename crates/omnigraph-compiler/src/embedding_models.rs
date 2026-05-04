#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingProvider {
    Gemini,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingModelSpec {
    pub name: &'static str,
    pub provider: EmbeddingProvider,
    pub dimensions: u32,
}

pub const DEFAULT_EMBEDDING_MODEL: &str = "gemini-embedding-2-preview";

pub const SUPPORTED_EMBEDDING_MODELS: &[EmbeddingModelSpec] = &[EmbeddingModelSpec {
    name: DEFAULT_EMBEDDING_MODEL,
    provider: EmbeddingProvider::Gemini,
    dimensions: 3072,
}];

pub fn embedding_model_by_name(name: &str) -> Option<&'static EmbeddingModelSpec> {
    SUPPORTED_EMBEDDING_MODELS
        .iter()
        .find(|spec| spec.name == name)
}

pub fn default_embedding_model() -> &'static EmbeddingModelSpec {
    embedding_model_by_name(DEFAULT_EMBEDDING_MODEL).expect("default embedding model is registered")
}

pub fn supported_embedding_model_names() -> String {
    SUPPORTED_EMBEDDING_MODELS
        .iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>()
        .join(", ")
}
