use once_cell::sync::Lazy;
use std::collections::HashMap;

pub fn get_file_extension(file_type: &str) -> Option<&'static str> {
    static MIME_TYPE_MAP: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
        let mut map = HashMap::new();

        // Images
        map.insert("image/png", "png");
        map.insert("image/jpeg", "jpeg");
        map.insert("image/jpg", "jpg");
        map.insert("image/gif", "gif");
        map.insert("image/bmp", "bmp");
        map.insert("image/tiff", "tiff");
        map.insert("image/svg+xml", "svg");
        map.insert("image/webp", "webp");
        map.insert("image/heif", "heif");
        map.insert("image/heic", "heic");
        map.insert("image/x-icon", "ico");
        map.insert("image/avif", "avif");

        // Archives
        map.insert("application/zip", "zip");
        map.insert("application/x-rar-compressed", "rar");
        map.insert("application/rar", "rar");
        map.insert("application/x-tar", "tar");
        map.insert("application/gzip", "gz");
        map.insert("application/x-7z-compressed", "7z");
        map.insert("application/x-bzip2", "bz2");
        map.insert("application/x-lzma", "lzma");
        map.insert("application/x-xz", "xz");
        map.insert("application/x-zstd", "zst");
        map.insert("application/x-cdlink", "vcd");
        map.insert("application/x-iso9660-image", "iso");
        map.insert("application/x-apple-diskimage", "dmg");

        // Audio
        map.insert("audio/mpeg", "mp3");
        map.insert("audio/wav", "wav");
        map.insert("audio/ogg", "ogg");
        map.insert("audio/opus", "opus");
        map.insert("audio/flac", "flac");
        map.insert("audio/aac", "aac");
        map.insert("audio/midi", "midi");
        map.insert("audio/x-midi", "midi");
        map.insert("audio/x-aac", "aac");

        // Video
        map.insert("video/mp4", "mp4");
        map.insert("video/mpeg", "mpeg");
        map.insert("video/ogg", "ogv");
        map.insert("video/webm", "webm");
        map.insert("video/x-ms-wmv", "wmv");
        map.insert("video/x-flv", "flv");
        map.insert("video/quicktime", "mov");
        map.insert("video/x-matroska", "mkv");
        map.insert("video/3gpp", "3gp");
        map.insert("video/mpegps", "mpegps");
        map.insert("video/avi", "avi");
        map.insert("video/x-msvideo", "avi");
        map.insert("video/x-m4v", "m4v");
        map.insert("video/x-dv", "dv");
        map.insert("video/x-matroska", "mkv");
        map.insert("video/x-ms-asf", "asf");

        // Text
        map.insert("text/plain", "txt");
        map.insert("text/css", "css");
        map.insert("text/html", "html");
        map.insert("text/xml", "xml");
        map.insert("text/csv", "csv");
        map.insert("text/markdown", "md");
        map.insert("text/x-python", "py");
        map.insert("text/x-java-source", "java");
        map.insert("text/x-c", "c");
        map.insert("text/x-c++", "cpp");
        map.insert("text/x-ruby", "rb");
        map.insert("text/x-shellscript", "sh");
        map.insert("text/x-perl", "pl");
        map.insert("text/x-bash", "sh");
        map.insert("text/x-log", "log");
        map.insert("text/x-pascal", "pas");

        // Documents
        map.insert("application/pdf", "pdf");
        map.insert("application/postscript", "ps");
        map.insert("application/eps", "eps");
        map.insert("application/vnd.adobe.photoshop", "psd");
        map.insert("application/vnd.adobe.illustrator", "ai");
        map.insert("application/msword", "doc");
        map.insert(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "docx",
        );
        map.insert("application/vnd.ms-excel", "xls");
        map.insert(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xlsx",
        );
        map.insert("application/vnd.ms-powerpoint", "ppt");
        map.insert(
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "pptx",
        );
        map.insert("application/vnd.ms-xpsdocument", "xps");
        map.insert("application/vnd.apple.keynote", "key");
        map.insert("application/vnd.apple.numbers", "numbers");
        map.insert("application/vnd.dxf", "dxf");
        map.insert("application/vnd.oasis.opendocument.text", "odt");
        map.insert("application/vnd.oasis.opendocument.spreadsheet", "ods");
        map.insert("application/vnd.oasis.opendocument.presentation", "odp");
        map.insert("application/epub+zip", "epub");
        map.insert("application/x-mobipocket-ebook", "mobi");
        map.insert("application/x-tex", "tex");
        map.insert("application/x-mstex", "tex");
        map.insert("application/x-rst", "rst");

        // Fonts
        map.insert("font/ttf", "ttf");
        map.insert("font/otf", "otf");
        map.insert("font/woff", "woff");
        map.insert("font/woff2", "woff2");
        map.insert("font/sfnt", "sfnt");
        map.insert("font/eot", "eot");

        // Executables and Scripts
        map.insert("application/x-msdownload", "exe");
        map.insert("application/x-java-archive", "jar");
        map.insert("application/x-shellscript", "sh");
        map.insert("application/x-perl", "pl");
        map.insert("application/x-ruby", "rb");
        map.insert("application/x-php", "php");
        map.insert("application/x-tex", "tex");
        map.insert("application/x-mstex", "tex");
        map.insert("application/x-wine-extension", "wine");
        map.insert("application/x-debian-package", "deb");
        map.insert("application/x-rpm", "rpm");

        // Programming File Types
        map.insert("text/x-c", "c");
        map.insert("text/x-c++", "cpp");
        map.insert("text/x-java-source", "java");
        map.insert("text/x-python", "py");
        map.insert("text/x-ruby", "rb");
        map.insert("text/x-perl", "pl");
        map.insert("text/x-shellscript", "sh");
        map.insert("text/x-php", "php");
        map.insert("text/x-makefile", "mk");
        map.insert("text/x-swift", "swift");
        map.insert("text/x-go", "go");
        map.insert("text/x-kotlin", "kt");
        map.insert("text/x-r", "r");
        map.insert("text/x-sql", "sql");
        map.insert("text/x-matlab", "m");
        map.insert("text/x-assembly", "asm");
        map.insert("text/x-dart", "dart");
        map.insert("text/x-typescript", "ts");
        map.insert("text/x-objective-c", "m");
        map.insert("text/x-lisp", "lisp");
        map.insert("text/x-haskell", "hs");
        map.insert("text/x-scala", "scala");
        map.insert("text/x-vhdl", "vhdl");
        map.insert("text/x-verilog", "v");
        map.insert("text/x-rust", "rs");
        map.insert("text/x-erlang", "erl");
        map.insert("text/x-tcl", "tcl");
        map.insert("text/x-scheme", "scm");
        map.insert("text/x-dylan", "dylan");
        map.insert("text/x-awk", "awk");
        map.insert("text/x-fsharp", "fs");
        map.insert("text/x-prolog", "pl");
        map.insert("text/x-sml", "sml");
        map.insert("text/x-nim", "nim");
        map.insert("text/x-crystal", "cr");
        map.insert("text/x-ocaml", "ml");
        map.insert("text/x-coffeescript", "coffee");
        map.insert("text/x-reason", "re");
        map.insert("text/x-hcl", "hcl");
        map.insert("text/x-vim", "vim");

        // APK
        map.insert("application/vnd.android.package-archive", "apk");

        map
    });

    MIME_TYPE_MAP.get(file_type).copied()
}
