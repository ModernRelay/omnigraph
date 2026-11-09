pub mod ast;
pub mod ast3;
pub mod checked;
pub mod codes;
pub mod descriptor;
pub mod diagnostic;
pub mod habits3;
pub mod lint;
pub mod parser;
pub mod parser3;
pub mod print3;
pub mod typecheck;
pub mod typeck3;

#[cfg(test)]
mod desugar3_tests;
#[cfg(test)]
mod parser3_proptest;
#[cfg(test)]
mod parser3_redteam;
#[cfg(test)]
mod parser3_tests;
#[cfg(test)]
mod typeck3_tests;
