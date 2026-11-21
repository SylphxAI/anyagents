//! CLI handler for `anyagents groups`.

use std::path::Path;

use crate::{agent, client, create, ui};

pub(crate) fn run(data: &Path, action: crate::GroupsCommand) -> Result<(), anyagents_core::Error> {
    match action {
        crate::GroupsCommand::Join { group, agent } => {
            let who = match agent
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                Some(a) => a,
                None => client::agent_identity()
                    .map(|id| id.to_string())
                    .ok_or_else(|| {
                        anyagents_core::Error::Message("groups join needs an agent name".into())
                    })?,
            };
            let agent = agent::named(data, &who)?;
            agent::join_group(data, &group, &agent.id)?;
            if ui::is_pretty() {
                ui::success(&format!("{} joined {group}", agent.name));
            } else {
                println!("{group}\t{}", agent.name);
            }
        }
        crate::GroupsCommand::Leave { group, agent } => {
            let who = match agent
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                Some(a) => a,
                None => client::agent_identity()
                    .map(|id| id.to_string())
                    .ok_or_else(|| {
                        anyagents_core::Error::Message("groups leave needs an agent name".into())
                    })?,
            };
            let agent = agent::named(data, &who)?;
            agent::leave_group(data, &group, &agent.id)?;
            if ui::is_pretty() {
                ui::success(&format!("{} left {group}", agent.name));
            } else {
                println!("{group}\t{}", agent.name);
            }
        }
        crate::GroupsCommand::Members { group } => {
            let members = agent::group_members(data, &group)?;
            if ui::is_pretty() {
                let role_map: std::collections::HashMap<String, Option<String>> =
                    anyagents_core::Store::open(data.join("state.db"))
                        .ok()
                        .and_then(|s| s.list_agents().ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|a| (a.name, a.role))
                        .collect();
                println!(
                    "\n  {}  {} members\n",
                    ui::bold_cyan(&format!("Group · {group}")),
                    ui::bold(&members.len().to_string())
                );
                for name in members {
                    let role = role_map.get(&name).and_then(|r| r.as_deref());
                    let display = match role.map(str::trim).filter(|s| !s.is_empty()) {
                        Some(r) => format!("{} {}", ui::bold(&name), ui::dim(&format!("· {r}"))),
                        None => ui::bold(&name),
                    };
                    println!("  • {display}");
                }
                println!();
                return Ok(());
            }
            for name in members {
                println!("{name}");
            }
        }
        crate::GroupsCommand::List => {
            let list = agent::list_groups(data)?;
            if ui::is_pretty() {
                if list.is_empty() {
                    println!("  {}", ui::dim("No groups found."));
                    return Ok(());
                }
                let role_map: std::collections::HashMap<String, Option<String>> =
                    anyagents_core::Store::open(data.join("state.db"))
                        .ok()
                        .and_then(|s| s.list_agents().ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|a| (a.name, a.role))
                        .collect();
                println!(
                    "\n  {}  {} total\n",
                    ui::bold_cyan("Groups"),
                    ui::bold(&list.len().to_string())
                );
                let mut table =
                    ui::Table::new(&[("GROUP", false), ("COUNT", true), ("MEMBERS", false)]);
                for (group, members) in list {
                    let formatted_members: Vec<String> = members
                        .iter()
                        .map(|m| {
                            let r = role_map.get(m).and_then(|v| v.as_deref());
                            create::format_agent_display(m, r)
                        })
                        .collect();
                    table.add_row(vec![
                        ui::bold(&ui::cyan(&group)),
                        ui::dim(&members.len().to_string()),
                        formatted_members.join(", "),
                    ]);
                }
                table.print();
                println!();
                return Ok(());
            }
            for (group, members) in list {
                println!("{group}\t{}", members.join(","));
            }
        }
    }
    Ok(())
}
