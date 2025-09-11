use napi::bindgen_prelude::*;
use napi_derive::napi;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
  collections::{HashMap, HashSet},
  sync::LazyLock,
};
use texting_robots::Robot;
use url::Url;

static FILE_EXTENSIONS: &[&str] = &[
  ".png", ".jpg", ".jpeg", ".gif", ".css", ".js", ".ico", ".svg", ".tiff", ".zip", ".exe", ".dmg",
  ".mp4", ".mp3", ".wav", ".pptx", ".xlsx", ".avi", ".flv", ".woff", ".ttf", ".woff2", ".webp",
  ".inc",
];

static FILE_EXT_SET: LazyLock<HashSet<&'static str>> =
  LazyLock::new(|| FILE_EXTENSIONS.iter().copied().collect());

#[derive(Deserialize)]
#[napi(object)]
pub struct FilterLinksCall {
  pub links: Vec<String>,
  pub limit: Option<i64>,
  pub max_depth: u32,
  pub base_url: String,
  pub initial_url: String,
  pub regex_on_full_url: bool,
  pub excludes: Vec<String>,
  pub includes: Vec<String>,
  pub allow_backward_crawling: bool,
  pub ignore_robots_txt: bool,
  pub robots_txt: String,
}

#[derive(Serialize)]
#[napi(object)]
pub struct FilterLinksResult {
  pub links: Vec<String>,
  pub denial_reasons: HashMap<String, String>,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct SitemapUrl {
  pub loc: Vec<String>,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct SitemapEntry {
  pub loc: Vec<String>,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct SitemapUrlset {
  pub url: Vec<SitemapUrl>,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct SitemapIndex {
  pub sitemap: Vec<SitemapEntry>,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct ParsedSitemap {
  pub urlset: Option<SitemapUrlset>,
  pub sitemapindex: Option<SitemapIndex>,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct SitemapInstruction {
  pub action: String,
  pub urls: Vec<String>,
  pub count: u32,
}

#[derive(Serialize, Debug)]
#[napi(object)]
pub struct SitemapProcessingResult {
  pub instructions: Vec<SitemapInstruction>,
  pub total_count: u32,
}

const URL_PARSE_ERROR: &str = "URL_PARSE_ERROR";
const DEPTH_LIMIT: &str = "DEPTH_LIMIT";
const EXCLUDE_PATTERN: &str = "EXCLUDE_PATTERN";
const INCLUDE_PATTERN: &str = "INCLUDE_PATTERN";
const BACKWARD_CRAWLING: &str = "BACKWARD_CRAWLING";
const ROBOTS_TXT: &str = "ROBOTS_TXT";
const FILE_TYPE: &str = "FILE_TYPE";

#[inline]
fn is_file(path: &str) -> bool {
  if let Some(dot_pos) = path.rfind('.') {
    let extension = &path[dot_pos..];
    FILE_EXT_SET.contains(extension)
  } else {
    false
  }
}

#[inline]
fn get_url_depth(path: &str) -> u32 {
  path
    .split('/')
    .filter(|segment| !segment.is_empty() && *segment != "index.php" && *segment != "index.html")
    .count() as u32
}

fn _filter_links(data: FilterLinksCall) -> std::result::Result<FilterLinksResult, String> {
  let limit = data.limit.map_or(usize::MAX, |x| x.max(0) as usize);
  if limit == 0 {
    return Ok(FilterLinksResult {
      links: Vec::new(),
      denial_reasons: HashMap::new(),
    });
  }

  let base_url = Url::parse(&data.base_url).map_err(|e| format!("Base URL parse error: {e}"))?;
  let initial_url =
    Url::parse(&data.initial_url).map_err(|e| format!("Initial URL parse error: {e}"))?;
  let initial_path = initial_url.path();

  let excludes_regex: Vec<Regex> = data
    .excludes
    .iter()
    .filter_map(|e| Regex::new(e).ok())
    .collect();
  let includes_regex: Vec<Regex> = data
    .includes
    .iter()
    .filter_map(|i| Regex::new(i).ok())
    .collect();

  let robot = if !data.ignore_robots_txt && !data.robots_txt.is_empty() {
    Robot::new("FireCrawlAgent", data.robots_txt.as_bytes())
      .ok()
      .or_else(|| Robot::new("FirecrawlAgent", data.robots_txt.as_bytes()).ok())
  } else {
    None
  };

  let mut result_links = Vec::new();
  let mut denial_reasons = HashMap::new();

  for link in data.links {
    if result_links.len() >= limit {
      break;
    }

    let url = match base_url.join(&link) {
      Ok(url) => url,
      Err(_) => {
        denial_reasons.insert(link, URL_PARSE_ERROR.to_string());
        continue;
      }
    };

    let path = url.path();

    if get_url_depth(path) > data.max_depth {
      denial_reasons.insert(link, DEPTH_LIMIT.to_string());
      continue;
    }

    if is_file(path) {
      denial_reasons.insert(link, FILE_TYPE.to_string());
      continue;
    }

    if !data.allow_backward_crawling && !path.starts_with(initial_path) {
      denial_reasons.insert(link, BACKWARD_CRAWLING.to_string());
      continue;
    }

    let match_target = if data.regex_on_full_url {
      url.as_str()
    } else {
      path
    };

    if !excludes_regex.is_empty() && excludes_regex.iter().any(|r| r.is_match(match_target)) {
      denial_reasons.insert(link, EXCLUDE_PATTERN.to_string());
      continue;
    }

    if !includes_regex.is_empty() && !includes_regex.iter().any(|r| r.is_match(match_target)) {
      denial_reasons.insert(link, INCLUDE_PATTERN.to_string());
      continue;
    }

    if let Some(ref robot) = robot {
      if !robot.allowed(url.as_str()) {
        denial_reasons.insert(link, ROBOTS_TXT.to_string());
        continue;
      }
    }

    result_links.push(link);
  }

  Ok(FilterLinksResult {
    links: result_links,
    denial_reasons,
  })
}

/// Filter links based on crawling rules and constraints.
#[napi]
pub fn filter_links(data: FilterLinksCall) -> Result<FilterLinksResult> {
  _filter_links(data)
    .map_err(|e| Error::new(Status::GenericFailure, format!("Filter links error: {e}")))
}

fn _parse_sitemap_xml(xml_content: &str) -> std::result::Result<ParsedSitemap, String> {
  let doc = roxmltree::Document::parse_with_options(
    xml_content,
    roxmltree::ParsingOptions {
      allow_dtd: true,
      ..Default::default()
    },
  )
  .map_err(|e| format!("XML parsing error: {e}"))?;
  let root = doc.root_element();

  match root.tag_name().name() {
    "sitemapindex" => {
      let sitemaps = root
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "sitemap")
        .filter_map(|sitemap_node| {
          sitemap_node
            .children()
            .find(|n| n.is_element() && n.tag_name().name() == "loc")
            .and_then(|loc_node| loc_node.text())
            .map(|loc_text| SitemapEntry {
              loc: vec![loc_text.to_string()],
            })
        })
        .collect();

      Ok(ParsedSitemap {
        urlset: None,
        sitemapindex: Some(SitemapIndex { sitemap: sitemaps }),
      })
    }
    "urlset" => {
      let urls = root
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "url")
        .filter_map(|url_node| {
          url_node
            .children()
            .find(|n| n.is_element() && n.tag_name().name() == "loc")
            .and_then(|loc_node| loc_node.text())
            .map(|loc_text| SitemapUrl {
              loc: vec![loc_text.to_string()],
            })
        })
        .collect();

      Ok(ParsedSitemap {
        urlset: Some(SitemapUrlset { url: urls }),
        sitemapindex: None,
      })
    }
    _ => Err("Invalid sitemap format: root element must be 'sitemapindex' or 'urlset'".to_string()),
  }
}

/// Parse XML sitemap content into structured data.
#[napi]
pub fn parse_sitemap_xml(xml_content: String) -> Result<ParsedSitemap> {
  _parse_sitemap_xml(&xml_content).map_err(|e| {
    Error::new(
      Status::GenericFailure,
      format!("Parse sitemap XML error: {e}"),
    )
  })
}

fn _process_sitemap(xml_content: &str) -> std::result::Result<SitemapProcessingResult, String> {
  let parsed = _parse_sitemap_xml(xml_content)?;
  let mut instructions = Vec::new();
  let mut total_count: u32 = 0;

  if let Some(sitemapindex) = parsed.sitemapindex {
    let sitemap_urls: Vec<String> = sitemapindex
      .sitemap
      .iter()
      .filter_map(|sitemap| {
        if !sitemap.loc.is_empty() {
          Some(sitemap.loc[0].trim().to_string())
        } else {
          None
        }
      })
      .collect();

    if !sitemap_urls.is_empty() {
      let count = sitemap_urls.len() as u32;
      instructions.push(SitemapInstruction {
        action: "recurse".to_string(),
        urls: sitemap_urls,
        count,
      });
      total_count += count;
    }
  } else if let Some(urlset) = parsed.urlset {
    let mut xml_sitemaps = Vec::new();
    let mut valid_urls = Vec::new();

    for url_entry in urlset.url {
      if !url_entry.loc.is_empty() {
        let url = url_entry.loc[0].trim();
        let url_lower = url.to_lowercase();
        if url_lower.ends_with(".xml") || url_lower.ends_with(".xml.gz") {
          xml_sitemaps.push(url.to_string());
        } else if let Ok(parsed_url) = Url::parse(url) {
          let path_lower = parsed_url.path().to_lowercase();
          if !is_file(&path_lower) {
            valid_urls.push(url.to_string());
          }
        }
      }
    }

    if !xml_sitemaps.is_empty() {
      let count = xml_sitemaps.len() as u32;
      instructions.push(SitemapInstruction {
        action: "recurse".to_string(),
        urls: xml_sitemaps,
        count,
      });
      total_count += count;
    }

    if !valid_urls.is_empty() {
      let count = valid_urls.len() as u32;
      instructions.push(SitemapInstruction {
        action: "process".to_string(),
        urls: valid_urls,
        count,
      });
      total_count += count;
    }
  }

  Ok(SitemapProcessingResult {
    instructions,
    total_count,
  })
}

/// Process sitemap XML and extract crawling instructions.
#[napi]
pub fn process_sitemap(xml_content: String) -> Result<SitemapProcessingResult> {
  _process_sitemap(&xml_content).map_err(|e| {
    Error::new(
      Status::GenericFailure,
      format!("Process sitemap error: {e}"),
    )
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_sitemap_xml_urlset() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/page1</loc>
  </url>
  <url>
    <loc>https://example.com/page2</loc>
  </url>
</urlset>"#;

    let result = _parse_sitemap_xml(xml_content).unwrap();
    assert!(result.urlset.is_some());
    let urlset = result.urlset.unwrap();
    assert_eq!(urlset.url.len(), 2);
    assert_eq!(urlset.url[0].loc[0], "https://example.com/page1");
    assert_eq!(urlset.url[1].loc[0], "https://example.com/page2");
  }

  #[test]
  fn test_parse_sitemap_xml_sitemapindex() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap1.xml</loc>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap2.xml</loc>
  </sitemap>
</sitemapindex>"#;

    let result = _parse_sitemap_xml(xml_content).unwrap();
    assert!(result.sitemapindex.is_some());
    let sitemapindex = result.sitemapindex.unwrap();
    assert_eq!(sitemapindex.sitemap.len(), 2);
    assert_eq!(
      sitemapindex.sitemap[0].loc[0],
      "https://example.com/sitemap1.xml"
    );
    assert_eq!(
      sitemapindex.sitemap[1].loc[0],
      "https://example.com/sitemap2.xml"
    );
  }

  #[test]
  fn test_parse_sitemap_xml_invalid_root() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<invalid xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/page1</loc>
  </url>
</invalid>"#;

    let result = _parse_sitemap_xml(xml_content);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Invalid sitemap format"));
  }

  #[test]
  fn test_parse_sitemap_xml_malformed() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/page1</loc>
  </url>
</urlset"#; // Missing closing >

    let result = _parse_sitemap_xml(xml_content);
    assert!(result.is_err());
  }

  #[test]
  fn test_process_sitemap_urlset() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/page1</loc>
  </url>
  <url>
    <loc>https://example.com/sitemap2.xml</loc>
  </url>
  <url>
    <loc>https://example.com/image.png</loc>
  </url>
</urlset>"#;

    let result = _process_sitemap(xml_content).unwrap();
    assert_eq!(result.instructions.len(), 2);

    let recurse_instruction = result
      .instructions
      .iter()
      .find(|i| i.action == "recurse")
      .unwrap();
    assert_eq!(recurse_instruction.urls.len(), 1);
    assert_eq!(
      recurse_instruction.urls[0],
      "https://example.com/sitemap2.xml"
    );

    let process_instruction = result
      .instructions
      .iter()
      .find(|i| i.action == "process")
      .unwrap();
    assert_eq!(process_instruction.urls.len(), 1);
    assert_eq!(process_instruction.urls[0], "https://example.com/page1");
  }

  #[test]
  fn test_process_sitemap_sitemapindex() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap1.xml</loc>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap2.xml</loc>
  </sitemap>
</sitemapindex>"#;

    let result = _process_sitemap(xml_content).unwrap();
    assert_eq!(result.instructions.len(), 1);
    assert_eq!(result.instructions[0].action, "recurse");
    assert_eq!(result.instructions[0].urls.len(), 2);
    assert_eq!(
      result.instructions[0].urls[0],
      "https://example.com/sitemap1.xml"
    );
    assert_eq!(
      result.instructions[0].urls[1],
      "https://example.com/sitemap2.xml"
    );
  }

  #[test]
  fn test_filter_links_normal_robots_txt() {
    let data = FilterLinksCall {
      links: vec![
        "https://example.com/allowed".to_string(),
        "https://example.com/disallowed".to_string(),
      ],
      limit: Some(10),
      includes: vec![],
      excludes: vec![],
      ignore_robots_txt: false,
      robots_txt: "User-agent: *\nDisallow: /disallowed".to_string(),
      max_depth: 10,
      base_url: "https://example.com".to_string(),
      initial_url: "https://example.com".to_string(),
      regex_on_full_url: false,
      allow_backward_crawling: true,
    };

    let result = _filter_links(data).unwrap();
    assert_eq!(result.links.len(), 1);
    assert_eq!(result.links[0], "https://example.com/allowed");
    assert!(result
      .denial_reasons
      .contains_key("https://example.com/disallowed"));
    assert_eq!(
      result
        .denial_reasons
        .get("https://example.com/disallowed")
        .unwrap(),
      "ROBOTS_TXT"
    );
  }

  #[test]
  fn test_filter_links_malformed_robots_txt() {
    let data = FilterLinksCall {
      links: vec!["https://example.com/test".to_string()],
      limit: Some(10),
      includes: vec![],
      excludes: vec![],
      ignore_robots_txt: false,
      robots_txt: "Invalid robots.txt content with \x00 null bytes and malformed syntax"
        .to_string(),
      max_depth: 10,
      base_url: "https://example.com".to_string(),
      initial_url: "https://example.com".to_string(),
      regex_on_full_url: false,
      allow_backward_crawling: true,
    };

    let result = _filter_links(data);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.links.len(), 1);
    assert_eq!(result.links[0], "https://example.com/test");
  }

  #[test]
  fn test_filter_links_non_utf8_robots_txt() {
    let mut non_utf8_bytes = vec![0xFF, 0xFE];
    non_utf8_bytes.extend_from_slice(b"User-agent: *\nDisallow: /blocked");
    let non_utf8_string = String::from_utf8_lossy(&non_utf8_bytes).to_string();

    let data = FilterLinksCall {
      links: vec!["https://example.com/allowed".to_string()],
      limit: Some(10),
      includes: vec![],
      excludes: vec![],
      ignore_robots_txt: false,
      robots_txt: non_utf8_string,
      max_depth: 10,
      base_url: "https://example.com".to_string(),
      initial_url: "https://example.com".to_string(),
      regex_on_full_url: false,
      allow_backward_crawling: true,
    };

    let result = _filter_links(data);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.links.len(), 1);
    assert_eq!(result.links[0], "https://example.com/allowed");
  }

  #[test]
  fn test_filter_links_char_boundary_issue() {
    let problematic_content = "User-agent: *\nDisallow: /\u{a0}test";

    let data = FilterLinksCall {
      links: vec!["https://example.com/test".to_string()],
      limit: Some(10),
      includes: vec![],
      excludes: vec![],
      ignore_robots_txt: false,
      robots_txt: problematic_content.to_string(),
      max_depth: 10,
      base_url: "https://example.com".to_string(),
      initial_url: "https://example.com".to_string(),
      regex_on_full_url: false,
      allow_backward_crawling: true,
    };

    let result = _filter_links(data);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.links.len(), 1);
    assert_eq!(result.links[0], "https://example.com/test");
  }

  #[test]
  fn test_is_file() {
    assert!(is_file("test.png"));
    assert!(is_file("script.js"));
    assert!(is_file("style.css"));
    assert!(!is_file("page"));
    assert!(!is_file("directory/"));
  }
}
