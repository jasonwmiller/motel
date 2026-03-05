use crate::cli::SkillInstallArgs;
use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

const SKILL_CONTENT: &str = include_str!("../skills/motel/SKILL.md");

pub fn run(args: SkillInstallArgs) -> Result<()> {
    let dest = if args.global {
        let home = std::env::var("HOME").context("HOME environment variable not set")?;
        PathBuf::from(home).join(".claude/skills/motel/SKILL.md")
    } else {
        PathBuf::from(".claude/skills/motel/SKILL.md")
    };

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    fs::write(&dest, SKILL_CONTENT)
        .with_context(|| format!("failed to write {}", dest.display()))?;

    println!("Skill installed to {}", dest.display());
    Ok(())
}
