use serde::Deserialize;
use serde_json::Error as JsonError;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Write};
use std::process::exit;
use std::time::Instant;

// ==== МОДЕЛИ ДЛЯ JSON ====

#[derive(Debug, Deserialize)]
struct ChatExport {
    name: Option<String>,
    #[serde(rename = "type")]
    chat_type: Option<String>,
    id: Option<i64>,
    messages: Vec<Message>,
}

#[derive(Debug, Deserialize)]
struct Message {
    id: i64,

    #[serde(rename = "type")]
    msg_type: String,

    date: String,

    #[serde(default)]
    date_unixtime: Option<String>,

    #[serde(default)]
    from: Option<String>,

    #[serde(default)]
    from_id: Option<String>,

    #[serde(default)]
    forwarded_from: Option<String>,

    #[serde(default)]
    forwarded_from_id: Option<String>,

    // text может быть строкой или массивом кусков
    #[serde(default)]
    text: Option<RawText>,

    #[serde(default)]
    text_entities: Vec<TextEntity>,

    // медиа
    #[serde(default)]
    photo: Option<String>,

    #[serde(default)]
    file: Option<String>,

    // тип медиа: "voice_message", "video_file", "audio_file", "animation", "sticker", ...
    #[serde(default)]
    media_type: Option<String>,

    #[serde(default)]
    mime_type: Option<String>,

    #[serde(default)]
    poll: Option<Poll>,
}

#[derive(Debug, Deserialize)]
struct TextEntity {
    #[serde(rename = "type")]
    kind: String,
    text: String,

    #[serde(default)]
    document_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Poll {
    question: String,

    #[serde(default)]
    closed: bool,

    #[serde(default)]
    total_voters: i64,

    #[serde(default)]
    answers: Vec<PollAnswer>,
}

#[derive(Debug, Deserialize)]
struct PollAnswer {
    text: String,
    voters: i64,

    #[serde(default)]
    chosen: bool,
}

/// text бывает:
/// - "строка"
/// - ["строка", {obj}, "строка"]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawText {
    Str(String),
    Parts(Vec<TextPart>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TextPart {
    Str(String),
    Entity(TextEntity),
}

impl RawText {
    fn to_plain(&self) -> String {
        match self {
            RawText::Str(s) => s.clone(),
            RawText::Parts(parts) => {
                let mut out = String::new();
                for p in parts {
                    match p {
                        TextPart::Str(s) => out.push_str(s),
                        TextPart::Entity(e) => out.push_str(&e.text),
                    }
                }
                out
            }
        }
    }
}

// ==== СТАТИСТИКА ====

#[derive(Default)]
struct Stats {
    total_messages: usize,
    messages_with_any_media: usize,

    photo_messages: usize,
    video_messages: usize,
    voice_messages: usize,
    audio_messages: usize,
    gif_messages: usize,
    sticker_messages: usize,
    file_messages: usize, // "голые" файлы без media_type

    poll_messages: usize,
    forwarded_messages: usize,
    link_messages: usize,

    per_author: HashMap<String, usize>,
}

// ==== MAIN ====

fn main() {
    let args: Vec<String> = env::args().collect();
    let input_path = args.get(1).map(|s| s.as_str()).unwrap_or("result.json");
    let output_path = args.get(2).map(|s| s.as_str()).unwrap_or("chat.txt");

    let start = Instant::now();

    if let Err(e) = run(input_path, output_path) {
        eprintln!("Фатальная ошибка: {e}");
        exit(1);
    }

    let duration = start.elapsed();
    println!(
        "\nВремя обработки: {} нс (~{} мс)",
        duration.as_nanos(),
        duration.as_millis()
    );
}

fn run(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);

    let export: ChatExport = match serde_json::from_reader(reader) {
        Ok(v) => v,
        Err(e) => {
            print_json_error(&e);
            return Err(Box::new(e));
        }
    };

    let mut stats = Stats::default();
    let mut out = File::create(output_path)?;

    for msg in &export.messages {
        if msg.msg_type != "message" {
            continue;
        }

        stats.total_messages += 1;

        let name = msg.from.as_deref().unwrap_or("Unknown");
        let from_id = msg.from_id.as_deref().unwrap_or("no_id");

        *stats
            .per_author
            .entry(name.to_string())
            .or_insert(0) += 1;

        if msg.forwarded_from.is_some() || msg.forwarded_from_id.is_some() {
            stats.forwarded_messages += 1;
        }

        let mut text_plain = msg
            .text
            .as_ref()
            .map(|t| t.to_plain())
            .unwrap_or_else(String::new);

        // если пусто, но есть опрос — покажем хоть что-то
        if text_plain.is_empty() {
            if let Some(poll) = &msg.poll {
                text_plain = format!("[опрос: {}]", poll.question);
            }
        }

        if text_plain.contains("http://") || text_plain.contains("https://") {
            stats.link_messages += 1;
        }

        // === МЕДИА ===
        let mut has_any_media = false;

        if msg.photo.is_some() {
            stats.photo_messages += 1;
            has_any_media = true;
        }

        if let Some(mt) = msg.media_type.as_deref() {
            match mt {
                "voice_message" => {
                    stats.voice_messages += 1;
                    has_any_media = true;
                }
                "video_file" => {
                    stats.video_messages += 1;
                    has_any_media = true;
                }
                "audio_file" => {
                    stats.audio_messages += 1;
                    has_any_media = true;
                }
                "animation" => {
                    // в экспорте так помечаются GIF/анимации
                    stats.gif_messages += 1;
                    has_any_media = true;
                }
                "sticker" => {
                    stats.sticker_messages += 1;
                    has_any_media = true;
                }
                _ => {
                    // остальные media_type можно потом отдельно выводить
                }
            }
        }

        // "просто файл": есть file, но нет media_type
        if msg.file.is_some() && msg.media_type.is_none() {
            stats.file_messages += 1;
            has_any_media = true;
        }

        if msg.poll.is_some() {
            stats.poll_messages += 1;
            has_any_media = true;
        }

        if has_any_media {
            stats.messages_with_any_media += 1;
        }

        writeln!(out, "{}({}): {}", name, from_id, text_plain)?;
    }

    // ==== ВЫВОД СТАТИСТИКИ ====

    let chat_name = export.name.as_deref().unwrap_or("<без имени>");
    println!("Чат: {}", chat_name);
    println!("Всего сообщений: {}", stats.total_messages);
    println!("  сообщений с чем-то медийным: {}", stats.messages_with_any_media);
    println!("    фотографии: {}", stats.photo_messages);
    println!("    видео: {}", stats.video_messages);
    println!("    голосовые: {}", stats.voice_messages);
    println!("    аудио: {}", stats.audio_messages);
    println!("    GIF / анимации: {}", stats.gif_messages);
    println!("    стикеры: {}", stats.sticker_messages);
    println!(
        "    файлы (доки/картинки без media_type): {}",
        stats.file_messages
    );
    println!("  опросов: {}", stats.poll_messages);
    println!("  пересланных сообщений: {}", stats.forwarded_messages);
    println!("  сообщений со ссылками: {}", stats.link_messages);
    println!("  уникальных авторов: {}", stats.per_author.len());
    println!();

    println!("Сообщения по участникам:");
    let mut authors: Vec<_> = stats.per_author.into_iter().collect();
    authors.sort_by(|a, b| b.1.cmp(&a.1));
    for (name, count) in authors {
        let percent = if stats.total_messages > 0 {
            (count as f64 / stats.total_messages as f64) * 100.0
        } else {
            0.0
        };
        println!("- {}: {} ({:.1}%)", name, count, percent);
    }

    println!("\nИстория чата записана в {}", output_path);

    Ok(())
}

fn print_json_error(e: &JsonError) {
    eprintln!("Ошибка парсинга JSON: {e}");
    eprintln!("Строка: {}, столбец: {}", e.line(), e.column());
}