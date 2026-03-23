pub(crate) const NOW_PARAM_NAME: &str = "__nanograph_now";

#[derive(Debug, Clone)]
pub struct QueryFile {
    pub queries: Vec<QueryDecl>,
}

#[derive(Debug, Clone)]
pub struct QueryDecl {
    pub name: String,
    pub description: Option<String>,
    pub instruction: Option<String>,
    pub params: Vec<Param>,
    pub match_clause: Vec<Clause>,
    pub return_clause: Vec<Projection>,
    pub order_clause: Vec<Ordering>,
    pub limit: Option<u64>,
    pub mutation: Option<Mutation>,
}

#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub enum Clause {
    Binding(Binding),
    Traversal(Traversal),
    Filter(Filter),
    Negation(Vec<Clause>),
}

#[derive(Debug, Clone)]
pub struct Binding {
    pub variable: String,
    pub type_name: String,
    pub prop_matches: Vec<PropMatch>,
}

#[derive(Debug, Clone)]
pub struct PropMatch {
    pub prop_name: String,
    pub value: MatchValue,
}

#[derive(Debug, Clone)]
pub enum MatchValue {
    Literal(Literal),
    Variable(String),
    Now,
}

#[derive(Debug, Clone)]
pub struct Traversal {
    pub src: String,
    pub edge_name: String,
    pub dst: String,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub left: Expr,
    pub op: CompOp,
    pub right: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Contains,
}

impl std::fmt::Display for CompOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "!="),
            Self::Gt => write!(f, ">"),
            Self::Lt => write!(f, "<"),
            Self::Ge => write!(f, ">="),
            Self::Le => write!(f, "<="),
            Self::Contains => write!(f, "contains"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    Now,
    PropAccess {
        variable: String,
        property: String,
    },
    Nearest {
        variable: String,
        property: String,
        query: Box<Expr>,
    },
    Search {
        field: Box<Expr>,
        query: Box<Expr>,
    },
    Fuzzy {
        field: Box<Expr>,
        query: Box<Expr>,
        max_edits: Option<Box<Expr>>,
    },
    MatchText {
        field: Box<Expr>,
        query: Box<Expr>,
    },
    Bm25 {
        field: Box<Expr>,
        query: Box<Expr>,
    },
    Rrf {
        primary: Box<Expr>,
        secondary: Box<Expr>,
        k: Option<Box<Expr>>,
    },
    Variable(String),
    Literal(Literal),
    Aggregate {
        func: AggFunc,
        arg: Box<Expr>,
    },
    AliasRef(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Date(String),
    DateTime(String),
    List(Vec<Literal>),
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Ordering {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Insert(InsertMutation),
    Update(UpdateMutation),
    Delete(DeleteMutation),
}

#[derive(Debug, Clone)]
pub struct InsertMutation {
    pub type_name: String,
    pub assignments: Vec<MutationAssignment>,
}

#[derive(Debug, Clone)]
pub struct UpdateMutation {
    pub type_name: String,
    pub assignments: Vec<MutationAssignment>,
    pub predicate: MutationPredicate,
}

#[derive(Debug, Clone)]
pub struct DeleteMutation {
    pub type_name: String,
    pub predicate: MutationPredicate,
}

#[derive(Debug, Clone)]
pub struct MutationAssignment {
    pub property: String,
    pub value: MatchValue,
}

#[derive(Debug, Clone)]
pub struct MutationPredicate {
    pub property: String,
    pub op: CompOp,
    pub value: MatchValue,
}
