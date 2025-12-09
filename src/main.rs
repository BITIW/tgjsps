use clap::Parser;
use simd_json::OwnedValue;

use chrono::{Datelike, NaiveDateTime, Timelike};
use ahash::AHashMap;
use memchr::memchr3;

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::process::exit;
use std::time::Instant;

//
// ===================== CLI =====================
//

#[derive(Parser, Debug)]
#[command(
    author = "ты",
    version,
    about = "Telegram JSON -> текстовый лог + статистика",
    long_about = None
)]
struct Cli {
    /// Входной JSON (экспорт из Telegram)
    #[arg(short = 'i', long = "input", default_value = "result.json")]
    input: String,

    /// Выходной текстовый лог чата
    #[arg(short = 'o', long = "output", default_value = "chat.txt")]
    output: String,

    /// Расширенная статистика (топ слов, активность, спамеры)
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Писать статистику в stat.txt вместо консоли
    #[arg(long = "txt")]
    stat_txt: bool,
}

//
// ===================== СТАТИСТИКА =====================
//

#[derive(Default)]
struct Stats {
    chat_name: String,

    total_messages: usize,
    messages_with_any_media: usize,

    photo_messages: usize,
    video_messages: usize,
    voice_messages: usize,
    audio_messages: usize,
    gif_messages: usize,
    sticker_messages: usize,
    file_messages: usize,

    poll_messages: usize,
    forwarded_messages: usize,
    link_messages: usize,

    per_author: AHashMap<String, usize>,

    // топ слов
    word_freq: AHashMap<String, usize>,
    word_freq_per_author: AHashMap<String, AHashMap<String, usize>>,

    // активность
    hour_hist: [usize; 24], // по часам
    day_hist: [usize; 32],  // по дню месяца (1..31)

    // спам: автор -> (текст -> количество)
    spam_map: AHashMap<String, AHashMap<String, usize>>,
}

//
// ===================== MAIN =====================
//

fn main() {
    let cli = Cli::parse();

    let start = Instant::now();

    let stats = match run(&cli.input, &cli.output, cli.verbose) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Фатальная ошибка: {e}");
            exit(1);
        }
    };

    if cli.stat_txt {
        if let Err(e) = write_stats_to_file("stat.txt", &stats, cli.verbose) {
            eprintln!("Ошибка записи stat.txt: {e}");
        } else {
            println!("Статистика записана в stat.txt");
        }
    } else {
        let stdout = io::stdout();
        let mut handle = BufWriter::new(stdout.lock());
        if let Err(e) = write_stats(&mut handle, &stats, cli.verbose) {
            eprintln!("Ошибка вывода статистики: {e}");
        }
    }

    println!("История чата записана в {}", cli.output);

    let dur = start.elapsed();
    println!(
        "Время обработки: {} нс (~{} мс)",
        dur.as_nanos(),
        dur.as_millis()
    );
}

//
// ===================== ОСНОВНОЙ ПАРСИНГ =====================
//

fn run(
    input_path: &str,
    output_path: &str,
    verbose: bool,
) -> Result<Stats, Box<dyn std::error::Error>> {
    let mut buf = std::fs::read(input_path)?;

    let root: OwnedValue =
        simd_json::to_owned_value(&mut buf).map_err(|e| format!("Ошибка парсинга JSON: {e}"))?;

    let root_obj = match &root {
        OwnedValue::Object(map) => map,
        _ => return Err("Корень JSON не объект".into()),
    };

    let mut stats = Stats::default();

    // имя чата
    let chat_name = root_obj
        .get("name")
        .and_then(|v| match v {
            OwnedValue::String(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("<без имени>");
    stats.chat_name = chat_name.to_string();

    // messages
    let messages_val = root_obj
        .get("messages")
        .ok_or("В корне нет поля \"messages\"")?;

    let messages = match messages_val {
        OwnedValue::Array(arr) => arr.as_ref(),
        _ => return Err("\"messages\" не массив".into()),
    };

    let file_out = File::create(output_path)?;
    let mut out = BufWriter::new(file_out);

    for msg_val in messages {
        let msg_obj = match msg_val {
            OwnedValue::Object(obj) => obj,
            _ => continue,
        };

        let msg_type = get_str_field(msg_obj, "type").unwrap_or("");
        if msg_type != "message" {
            continue;
        }

        stats.total_messages += 1;

        let name = get_str_field(msg_obj, "from").unwrap_or("Unknown");
        let from_id = get_str_field(msg_obj, "from_id").unwrap_or("no_id");

        *stats.per_author.entry(name.to_string()).or_insert(0) += 1;

        if msg_obj.get("forwarded_from").is_some()
            || msg_obj.get("forwarded_from_id").is_some()
        {
            stats.forwarded_messages += 1;
        }

        // ===== дата -> активность (ТОЛЬКО при verbose) =====
        if verbose {
            if let Some(OwnedValue::String(date_str)) = msg_obj.get("date") {
                if let Ok(dt) =
                    NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S")
                {
                    let h = dt.hour() as usize;
                    if h < 24 {
                        stats.hour_hist[h] += 1;
                    }
                    let d = dt.day() as usize;
                    if d < stats.day_hist.len() {
                        stats.day_hist[d] += 1;
                    }
                }
            }
        }

        // префикс "name(id): "
        out.write_all(name.as_bytes())?;
        out.write_all(b"(")?;
        out.write_all(from_id.as_bytes())?;
        out.write_all(b"): ")?;

        // ===== текст =====
        let mut has_any_text = false;

        if let Some(text_val) = msg_obj.get("text") {
            if verbose {
                // тяжёлый путь: без лишних String для слов, но со спамом
                if !text_is_empty(text_val) {
                    if text_has_link(text_val) {
                        stats.link_messages += 1;
                    }

                    // вывод как в не-verbose
                    write_text_value(text_val, &mut out)?;
                    has_any_text = true;

                    // слова по сегментам текста
                    update_word_stats(&mut stats, name, text_val);
                    // спам по целому тексту
                    track_spam(&mut stats, name, text_val);
                }
            } else {
                // лёгкий путь: вообще без String
                if !text_is_empty(text_val) {
                    if text_has_link(text_val) {
                        stats.link_messages += 1;
                    }
                    write_text_value(text_val, &mut out)?;
                    has_any_text = true;
                }
            }
        }

        // если текста нет, но есть опрос — выводим вопрос
        if !has_any_text {
            if let Some(poll_val) = msg_obj.get("poll") {
                if let Some(q) = get_poll_question(poll_val) {
                    out.write_all("[опрос: ".as_bytes())?;
                    out.write_all(q.as_bytes())?;
                    out.write_all(b"]")?;
                }
            }
        }

        // ======== медиа ========
        let mut has_any_media = false;

        if msg_obj.get("photo").is_some() {
            stats.photo_messages += 1;
            has_any_media = true;
        }

        if let Some(OwnedValue::String(mt)) = msg_obj.get("media_type") {
            match mt.as_str() {
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
                    stats.gif_messages += 1;
                    has_any_media = true;
                }
                "sticker" => {
                    stats.sticker_messages += 1;
                    has_any_media = true;
                }
                _ => {}
            }
        }

        if msg_obj.get("file").is_some() && !msg_obj.contains_key("media_type") {
            stats.file_messages += 1;
            has_any_media = true;
        }

        if msg_obj.get("poll").is_some() {
            stats.poll_messages += 1;
            has_any_media = true;
        }

        if has_any_media {
            stats.messages_with_any_media += 1;
        }

        out.write_all(b"\n")?;
    }

    Ok(stats)
}

//
// ===================== ХЕЛПЕРЫ ПО JSON =====================
//

fn get_str_field<'a>(
    obj: &'a simd_json::owned::Object,
    key: &str,
) -> Option<&'a str> {
    obj.get(key).and_then(|v| match v {
        OwnedValue::String(s) => Some(s.as_str()),
        _ => None,
    })
}

// обход всех текстовых сегментов (строки и obj["text"])
fn for_each_text_segment<'a, F>(v: &'a OwnedValue, mut f: F)
where
    F: FnMut(&'a str),
{
    match v {
        OwnedValue::String(s) => f(s.as_str()),
        OwnedValue::Array(arr) => {
            for part in arr.as_ref().iter() {
                match part {
                    OwnedValue::String(s) => f(s.as_str()),
                    OwnedValue::Object(obj) => {
                        if let Some(OwnedValue::String(t)) = obj.get("text") {
                            f(t.as_str());
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

// лёгкая запись текста без аллокаций (используется и в обычном, и в verbose)
fn write_text_value<W: Write>(v: &OwnedValue, w: &mut W) -> io::Result<()> {
    let mut res: io::Result<()> = Ok(());
    for_each_text_segment(v, |s| {
        if res.is_ok() {
            if let Err(e) = w.write_all(s.as_bytes()) {
                res = Err(e);
            }
        }
    });
    res
}

fn text_is_empty(v: &OwnedValue) -> bool {
    let mut any = false;
    let mut all_empty = true;
    for_each_text_segment(v, |s| {
        any = true;
        if !s.is_empty() {
            all_empty = false;
        }
    });
    if !any {
        true
    } else {
        all_empty
    }
}

fn text_has_link(v: &OwnedValue) -> bool {
    fn has_link_str(s: &str) -> bool {
        s.contains("http://") || s.contains("https://")
    }

    let mut res = false;
    for_each_text_segment(v, |s| {
        if has_link_str(s) {
            res = true;
        }
    });
    res
}

fn get_poll_question(poll_val: &OwnedValue) -> Option<&str> {
    match poll_val {
        OwnedValue::Object(obj) => obj
            .get("question")
            .and_then(|v| match v {
                OwnedValue::String(s) => Some(s.as_str()),
                _ => None,
            }),
        _ => None,
    }
}

//
// ===================== СВОЙ ТОКЕНИЗАТОР + СЛОВА + СПАМ =====================
//

/// Быстрая токенизация по ASCII-пробелам/табам/переводам строки.
/// Колбэк получает &str без аллокаций.
fn fast_tokenize(text: &str, mut cb: impl FnMut(&str)) {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // пропускаем ASCII-пробелы
        while i < len && is_ascii_ws(bytes[i]) {
            i += 1;
        }
        if i >= len {
            break;
        }

        let start = i;

        // ищем следующий пробельный байт space / \n / \t
        let slice = &bytes[i..];
        let rel = memchr3(b' ', b'\n', b'\t', slice).unwrap_or(slice.len());
        let end = i + rel;

        i = end;

        let token = &text[start..end];
        cb(token);
    }
}

#[inline]
fn is_ascii_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\t' | b'\r')
}

/// Обрезаем ASCII-пунктуацию по краям, всё не-ASCII считаем частью слова.
fn trim_ascii_punct(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut end = bytes.len();

    // слева
    while start < end {
        let b = bytes[start];
        if b < 0x80 && !is_ascii_word_char(b) {
            start += 1;
        } else {
            break;
        }
    }

    // справа
    while end > start {
        let b = bytes[end - 1];
        if b < 0x80 && !is_ascii_word_char(b) {
            end -= 1;
        } else {
            break;
        }
    }

    &s[start..end]
}

#[inline]
fn is_ascii_word_char(b: u8) -> bool {
    (b'A'..=b'Z').contains(&b)
        || (b'a'..=b'z').contains(&b)
        || (b'0'..=b'9').contains(&b)
        || b == b'#'
        || b == b'@'
        || b == b'_'
}

// считаем слова по сегментам текста, без общего String
fn update_word_stats(stats: &mut Stats, author: &str, text_val: &OwnedValue) {
    for_each_text_segment(text_val, |segment| {
        fast_tokenize(segment, |raw| {
            let token = trim_ascii_punct(raw);
            if token.len() < 3 {
                return;
            }

            if token.starts_with("http://") || token.starts_with("https://") {
                return;
            }

            let token_lower = token.to_lowercase();
            if token_lower.is_empty() {
                return;
            }

            *stats.word_freq.entry(token_lower.clone()).or_insert(0) += 1;

            let per_author = stats
                .word_freq_per_author
                .entry(author.to_string())
                .or_default();
            *per_author.entry(token_lower).or_insert(0) += 1;
        });
    });
}

// строим полный текст ТОЛЬКО для спама
fn build_full_text(v: &OwnedValue) -> String {
    let mut out = String::new();
    for_each_text_segment(v, |s| {
        out.push_str(s);
    });
    out
}

fn track_spam(stats: &mut Stats, author: &str, text_val: &OwnedValue) {
    let full = build_full_text(text_val);
    let norm = full.trim().to_lowercase();
    if norm.len() < 5 {
        return;
    }

    let entry = stats.spam_map.entry(author.to_string()).or_default();
    *entry.entry(norm).or_insert(0) += 1;
}

//
// ===================== ВЫВОД СТАТЫ =====================
//

fn write_stats_to_file(
    path: &str,
    stats: &Stats,
    verbose: bool,
) -> io::Result<()> {
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    write_stats(&mut w, stats, verbose)
}

fn write_stats<W: Write>(
    w: &mut W,
    stats: &Stats,
    verbose: bool,
) -> io::Result<()> {
    writeln!(w, "Чат: {}", stats.chat_name)?;
    writeln!(w, "Всего сообщений: {}", stats.total_messages)?;
    writeln!(
        w,
        "  сообщений с чем-то медийным: {}",
        stats.messages_with_any_media
    )?;
    writeln!(w, "    фотографии: {}", stats.photo_messages)?;
    writeln!(w, "    видео: {}", stats.video_messages)?;
    writeln!(w, "    голосовые: {}", stats.voice_messages)?;
    writeln!(w, "    аудио: {}", stats.audio_messages)?;
    writeln!(w, "    GIF / анимации: {}", stats.gif_messages)?;
    writeln!(w, "    стикеры: {}", stats.sticker_messages)?;
    writeln!(
        w,
        "    файлы (без media_type): {}",
        stats.file_messages
    )?;
    writeln!(w, "  опросов: {}", stats.poll_messages)?;
    writeln!(w, "  пересланных сообщений: {}", stats.forwarded_messages)?;
    writeln!(w, "  сообщений со ссылками: {}", stats.link_messages)?;
    writeln!(w, "  уникальных авторов: {}", stats.per_author.len())?;
    writeln!(w)?;

    // авторы
    writeln!(w, "Сообщения по участникам:")?;
    let mut authors: Vec<_> = stats.per_author.iter().collect();
    authors.sort_by(|a, b| b.1.cmp(a.1));
    for (name, count) in authors {
        let percent = if stats.total_messages > 0 {
            (*count as f64 / stats.total_messages as f64) * 100.0
        } else {
            0.0
        };
        writeln!(w, "- {}: {} ({:.1}%)", name, count, percent)?;
    }

    if verbose {
        // ========== Топ слов (глобально) ==========
        writeln!(w)?;
        writeln!(w, "Топ слов (глобально):")?;
        let mut words: Vec<_> = stats.word_freq.iter().collect();
        words.sort_by(|a, b| b.1.cmp(a.1));
        for (word, count) in words.into_iter().take(20) {
            writeln!(w, "- {}: {}", word, count)?;
        }

        // ========== Активность по часам ==========
        writeln!(w)?;
        writeln!(w, "Активность по часам (0–23):")?;
        let mut best_hour = 0usize;
        let mut best_hour_count = 0usize;
        for hour in 0..24 {
            let c = stats.hour_hist[hour];
            if c > best_hour_count {
                best_hour_count = c;
                best_hour = hour;
            }
            writeln!(w, "  {:02}:00–{:02}:59: {}", hour, hour, c)?;
        }
        writeln!(
            w,
            "Самый активный час: {:02}:00–{:02}:59 ({} сообщений)",
            best_hour, best_hour, best_hour_count
        )?;

        // ========== Активность по дням месяца ==========
        writeln!(w)?;
        writeln!(w, "Активность по дням месяца:")?;
        let mut best_day = 1usize;
        let mut best_day_count = 0usize;
        for day in 1..stats.day_hist.len() {
            let c = stats.day_hist[day];
            if c > best_day_count {
                best_day_count = c;
                best_day = day;
            }
            writeln!(w, "  {:02}: {}", day, c)?;
        }
        writeln!(
            w,
            "Самый активный день месяца: {:02} ({} сообщений)",
            best_day, best_day_count
        )?;

        // ========== Спам ==========
        writeln!(w)?;
        writeln!(
            w,
            "Потенциальные спамеры (повторяющийся одинаковый текст):"
        )?;
        let mut spam_scores: Vec<(String, usize)> = Vec::new();
        for (author, msgs) in &stats.spam_map {
            let mut extra = 0usize;
            for &count in msgs.values() {
                if count > 1 {
                    extra += count - 1;
                }
            }
            if extra > 0 {
                spam_scores.push((author.clone(), extra));
            }
        }
        spam_scores.sort_by(|a, b| b.1.cmp(&a.1));
        for (author, extra) in spam_scores.into_iter().take(10) {
            writeln!(w, "- {}: {} дополнительных повторов", author, extra)?;
        }
    }

    Ok(())
}
