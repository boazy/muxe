// Minimized from the exact pinned source: zellij-utils/src/position.rs

#[derive(Debug, Hash, Copy, Clone, PartialEq, Eq, PartialOrd, Deserialize, Serialize)]
pub struct Position {
    pub line: Line,
    pub column: Column,
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, PartialOrd)]
pub struct Line(pub isize);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, PartialOrd)]
pub struct Column(pub usize);
