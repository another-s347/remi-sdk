use super::*;

impl<'a> ThingsLocalService<'a> {
    pub fn splice_text(
        &self,
        device_id: &str,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<ThingsMutationResult<bool>> {
        self.splice_text_with_context(
            ThingsMutationContext::local_command(device_id),
            thing_uuid,
            block_id,
            index,
            delete,
            insert,
        )
    }

    pub fn splice_text_with_context(
        &self,
        context: ThingsMutationContext,
        thing_uuid: &str,
        block_id: &str,
        index: usize,
        delete: usize,
        insert: &str,
    ) -> Result<ThingsMutationResult<bool>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before text splice")?;

        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;
        if !snapshot.things.iter().any(|thing| thing.uuid == thing_uuid) {
            tracing::debug!(
                thing_uuid,
                "things_splice_text: refusing to edit deleted or unreachable thing"
            );
            return Ok(ThingsMutationResult {
                value: false,
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        }

        let Some(events) =
            doc_set.try_splice_thing_text(thing_uuid, block_id, index, delete, insert)?
        else {
            return Ok(ThingsMutationResult {
                value: false,
                events: Vec::new(),
                dirty_documents: Vec::new(),
                change_log_ids: Vec::new(),
                event_range: None,
            });
        };
        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, true)?;

        if self
            .storage
            .find_recent_thing_update_log(thing_uuid, 300)
            .ok()
            .flatten()
            .is_none()
        {
            let summary = format!("Edited thing content");
            let details = json!({
                "uuid": thing_uuid,
                "block_id": block_id,
                "operation": "splice_text",
            });
            self.record_user_change_log(
                &context,
                &mut result.change_log_ids,
                ThingsOperationType::UpdateThing,
                "thing",
                thing_uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            );
        }

        Ok(result)
    }

    pub fn edit_content(
        &self,
        device_id: &str,
        thing_uuid: &str,
        operation: &str,
        new_title: Option<&str>,
        new_content: Option<&str>,
        old_str: Option<&str>,
        new_str: Option<&str>,
        line_number: Option<usize>,
        insert_text: Option<&str>,
        append_text: Option<&str>,
    ) -> Result<ThingsMutationResult<String>> {
        self.edit_content_with_context(
            ThingsMutationContext::local_command(device_id),
            thing_uuid,
            operation,
            new_title,
            new_content,
            old_str,
            new_str,
            line_number,
            insert_text,
            append_text,
        )
    }

    pub fn edit_content_with_context(
        &self,
        context: ThingsMutationContext,
        thing_uuid: &str,
        operation: &str,
        new_title: Option<&str>,
        new_content: Option<&str>,
        old_str: Option<&str>,
        new_str: Option<&str>,
        line_number: Option<usize>,
        insert_text: Option<&str>,
        append_text: Option<&str>,
    ) -> Result<ThingsMutationResult<String>> {
        let mut doc_set = DocumentPersistence::new(self.storage)
            .load_or_init_document_set(&context.device_id)
            .context("Failed to load Things document set before content edit")?;

        let snapshot =
            doc_set.extract_snapshot_with_options(crate::things_crdt::SnapshotOptions {
                include_content: false,
            })?;

        let Some(thing) = snapshot
            .things
            .iter()
            .find(|thing| thing.uuid == thing_uuid)
            .cloned()
        else {
            return Ok(no_event_result(
                json!({
                    "error": "thing_not_found",
                    "message": format!("Thing with UUID '{}' not found", thing_uuid),
                })
                .to_string(),
            ));
        };

        let include_content_for_read = operation != "overwrite";
        let current_markdown = if include_content_for_read {
            doc_set
                .get_thing_markdown_text(thing_uuid)?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let (final_content, title_only) = match operation {
            "overwrite" => {
                let content = new_content.unwrap_or("");
                (content.to_string(), false)
            }
            "set_title" => (current_markdown.clone(), true),
            "str_replace" => {
                let old = old_str.ok_or_else(|| anyhow!("str_replace requires 'old_str'"))?;
                let new = new_str.unwrap_or("");
                let matches: Vec<_> = current_markdown.match_indices(old).collect();

                if matches.is_empty() {
                    return Ok(no_event_result(
                        json!({
                            "error": "str_replace_no_match",
                            "message": format!("'old_str' not found in content"),
                            "current_content": current_markdown,
                            "old_str": old,
                        })
                        .to_string(),
                    ));
                }

                if matches.len() > 1 {
                    return Ok(no_event_result(json!({
                        "error": "str_replace_multiple_matches",
                        "message": format!("'old_str' found {} times, must be unique. Use more context.", matches.len()),
                        "current_content": current_markdown,
                        "old_str": old,
                        "match_positions": matches.iter().map(|(pos, _)| *pos).collect::<Vec<_>>(),
                    }).to_string()));
                }

                (current_markdown.replacen(old, new, 1), false)
            }
            "insert_at_line" => {
                let line_num = line_number.unwrap_or(0);
                let insert =
                    insert_text.ok_or_else(|| anyhow!("insert_at_line requires 'insert_text'"))?;

                let lines: Vec<&str> = current_markdown.lines().collect();
                let total_lines = lines.len();

                if line_num > total_lines {
                    return Ok(no_event_result(json!({
                        "error": "insert_at_line_out_of_range",
                        "message": format!("Line {} is out of range. Content has {} lines.", line_num, total_lines),
                        "current_content": current_markdown,
                        "total_lines": total_lines,
                    }).to_string()));
                }

                let mut new_lines: Vec<&str> = Vec::with_capacity(lines.len() + 1);
                if line_num == 0 {
                    new_lines.push(insert);
                    new_lines.extend(lines);
                } else {
                    for (i, line) in lines.iter().enumerate() {
                        new_lines.push(line);
                        if i + 1 == line_num {
                            new_lines.push(insert);
                        }
                    }
                }
                (new_lines.join("\n"), false)
            }
            "append" => {
                let append = append_text.ok_or_else(|| anyhow!("append requires 'append_text'"))?;
                let mut result = current_markdown.clone();
                if !result.is_empty() && !result.ends_with('\n') {
                    result.push('\n');
                }
                result.push_str(append);
                (result, false)
            }
            _ => {
                return Ok(no_event_result(json!({
                    "error": "invalid_operation",
                    "message": format!("Unknown operation '{}'. Valid: overwrite, set_title, str_replace, insert_at_line, append", operation),
                }).to_string()));
            }
        };

        let final_title = new_title.unwrap_or(&thing.title);
        let mut events = Vec::new();

        if new_title.is_some() || operation == "set_title" {
            events.extend(doc_set.upsert_thing_meta(
                &thing.collection_uuid,
                thing_uuid,
                None,
                None,
                Some(final_title.to_string()),
                thing.parent_uuid.clone(),
                remi_things_crdt::TriggerUpdate::Noop,
            )?);
        }

        if !title_only {
            events.extend(doc_set.replace_thing_markdown_text(thing_uuid, &final_content)?);
        }

        let result_content = if title_only {
            current_markdown
        } else {
            final_content
        };
        let value = json!({
            "success": true,
            "uuid": thing_uuid,
            "title": final_title,
            "operation": operation,
            "content": result_content,
        })
        .to_string();

        let mut result =
            self.pipeline
                .commit_local_documents(&context, &mut doc_set, events, value)?;

        if self
            .storage
            .find_recent_thing_update_log(thing_uuid, 300)
            .ok()
            .flatten()
            .is_none()
        {
            let summary = format!("Edited thing '{}' ({})", final_title, operation);
            let details = json!({
                "uuid": thing_uuid,
                "operation": operation,
            });
            self.record_user_change_log(
                &context,
                &mut result.change_log_ids,
                ThingsOperationType::UpdateThing,
                "thing",
                thing_uuid,
                &summary,
                &details.to_string(),
                None,
                true,
            );
        }

        Ok(result)
    }
}
