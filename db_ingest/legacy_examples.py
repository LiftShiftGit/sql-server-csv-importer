"""Authored synthetic exports. No importer parser/converter computes expectations.
Names describe conventions, not actual private data. Standard codecs encode authored text.
"""
LEGACY_TEXT = {
    "ascii": "ASCII only  ", "cp1252": "Caf\u00e9 \u20ac \u201cquoted\u201d  ",
    "iso8859-1": "Caf\u00e9 \u00a3 \u00ff  ", "iso8859-15": "Caf\u00e9 \u20ac \u0153  ",
    "cp437": "DOS \u250c\u2500\u2510 \u00e9  ", "cp850": "DOS \u00f8 \u00a3 \u00e9  ",
    "cp858": "DOS \u20ac \u00f8  ", "cp1250": "\u0141\u00f3d\u017a \u010cesk\u00fd  ",
    "cp1251": "\u041c\u043e\u0441\u043a\u0432\u0430 \u042f  ", "koi8-r": "\u041c\u043e\u0441\u043a\u0432\u0430 \u042f  ",
    "cp1253": "\u0391\u03b8\u03ae\u03bd\u03b1  ", "cp1254": "\u0130stanbul \u015f  ",
    "cp1255": "\u05e9\u05dc\u05d5\u05dd  ", "cp1256": "\u0645\u0631\u062d\u0628\u0627  ",
    "cp1257": "R\u012bga \u017e  ", "cp1258": "Vi\u00ea\u0323t  ",
}

def examples(category):
    def row(name, content, expected, **options):
        return dict(name=name, content=content, expected=expected, **options)
    if category == "csv_legacy_encodings":
        for encoding, text in LEGACY_TEXT.items():
            yield row("encoding_"+encoding+".csv", ('code,label\r\n001,"'+text+'"\r\n').encode(encoding), [["001",text]], enc=encoding)
        for encoding in ("utf-8-sig","utf-16-le","utf-16-be","utf-32-le","utf-32-be"):
            text="\u4e2d\u6587 \U0001f600 e\u0301  "
            yield row("encoding_"+encoding+".csv", ('code,label\r\n001,"'+text+'"\r\n').encode(encoding), [["001",text]], enc=encoding)
        for encoding,text in (("shift_jis","東京 ソフト"),("cp932","東京 ①"),("euc_jp","東京 ソフト"),("gb18030","中文 😀"),("gbk","中文 北京"),("big5","中文 臺北"),("euc_kr","서울 한국"),("cp949","서울 한국")):
            yield row("encoding_"+encoding+".csv",('code,label\r\n001,"'+text+'  "\r\n').encode(encoding),[["001",text+"  "]],enc=encoding)
    elif category == "mainframe_ebcdic":
        for encoding in ("cp037","cp500","cp1140"):
            text="Caf\u00e9 []{}|!^~  " + ("\u20ac" if encoding=="cp1140" else "\u00a4")
            for ending, label in (("\n","lf"),("\r\n","crlf"),("\x85","nel")):
                raw=('code;label'+ending+'000123;"'+text+'"'+ending).encode(encoding)
                yield row("ebcdic_"+encoding+"_"+label+".csv",raw,[["000123",text]],enc=encoding,sep=";",overrides={"nel_is_newline":ending=="\x85"},notes=["Synthetic DISPLAY text export, not a COBOL binary record. Original code page and padding are retained."])
        yield row("ebcdic_multiline_nel.csv", 'code,label\x85001,"first\x85second, ""quoted"""\x85'.encode("cp037"), [["001",'first\x85second, "quoted"']],enc="cp037",overrides={"nel_is_newline":True})
        yield row("ebcdic_display_numbers.csv",'code,amount,overpunch\n000123,-1234.50,123D\n'.encode("cp037"),[["000123","-1234.50","123D"]],enc="cp037",notes=["DISPLAY decimal can use an explicit decimal column rule. Overpunch remains text; no implicit sign interpretation."])
    elif category == "csv_escaping":
        yield row("escape_rfc.csv",'code,label\r\n001,"a,b;|\t"\r\n002,"said ""hello"""\r\n003,""\r\n',[["001","a,b;|\t"],["002",'said "hello"'],["003",""]])
        yield row("escape_backslash.csv",b'code,label\n001,"say \\"hi\\" and C:\\\\temp"\n',[["001",'say "hi" and C:\\temp']],overrides={"escapechar":"\\","doublequote":False})
        yield row("escape_unquoted.csv",b'code,label\n001,a\\,b\n002,path\\\\file\n',[["001","a,b"],["002","path\\file"]],overrides={"quotechar":"none","escapechar":"\\"})
        yield row("escape_single_quote.csv","code;label\n001;'a;b and ''quote'''\n",[["001","a;b and 'quote'"]],sep=";",overrides={"quotechar":"'"})
        yield row("escape_multiline.csv",b'code,label\r\n001,"CR\rLF\nCRLF\r\nend"\r\n',[["001","CR\rLF\nCRLF\r\nend"]])
        yield row("escape_mixed_endings.csv",b'code,label\r001,one\r\n002,two\n003,last',[["001","one"],["002","two"],["003","last"]])
        yield row("escape_spaces.csv",'code,label\n001,  spaced  \n002, "literal quotes"\n003,a"b\n',[["001","  spaced  "],["002",' "literal quotes"'],["003",'a"b']])
        yield row("escape_trailing_empty.csv",b'code,label,extra\n001,,\n002,"",NULL\n',[["001","",""] ,["002","",{"type":"null","value":None}]],overrides={"null_tokens":["NULL"]})
        yield row("escape_literal_backslash.csv",b'code,label\n001,C:\\temp\\new\n',[["001","C:\\temp\\new"]])
        yield row("escape_nel_literal.csv",'code,label\n001,"NEL\x85is data"\n',[["001","NEL\x85is data"]])
    elif category == "csv_dirty":
        def authored(ordinal,*values):
            return {"ordinal":ordinal,"values":[{"type":"text","value":v} for v in values]}
        yield row("dirty_ragged.csv",b'code,label\n001,ok\n002\n003,too,many\n004,after\n',[authored(2,"001","ok"),authored(5,"004","after")],outcome="rejected",rejected=[3,4])
        yield row("dirty_postquote.csv",b'code,label\n001,"bad"junk\n002,ok\n',[authored(3,"002","ok")],outcome="rejected",rejected=[2],reason_codes=["CSV_INVALID"])
        yield row("dirty_blank.csv",b'code,label\n\n001,ok\n',[authored(3,"001","ok")],outcome="rejected",rejected=[2],reason_codes=["BLANK_RECORD"])
        yield row("dirty_unclosed.csv",b'code,label\n001,ok\n002,"unclosed\n003,looks-valid\n',[["001","ok"]],outcome="unresolved",unknown=True)
        yield row("dirty_dangling_escape.csv",b'code,label\n001,bad\\',[],outcome="unresolved",unknown=True,overrides={"escapechar":"\\"})
        for name, raw, encoding in (("utf8",b'code,label\n001,\xff\n',"utf-8"),("mixed_encoding",b'code,label\n001,Caf\xe9\n',"utf-8"),("undefined_cp1252",b'code,label\n001,\x81\n',"cp1252"),("truncated_utf16",b'c\x00o\x00d\x00e\x00,\x00x\x00\n\x00\x31',"utf-16-le")):
            yield row("dirty_"+name+".csv",raw,[],enc=encoding,outcome="encoding_failure",unknown=True)
        yield row("dirty_nul.csv",b'code,label\n001,bad\x00value\n',[],outcome="unsupported_format",unknown=True,reason_codes=["UNSUPPORTED_FORMAT"])
        yield row("dirty_bom_conflict.csv",b'\xef\xbb\xbfcode,label\n001,ok\n',[],enc="cp1252",outcome="encoding_failure",unknown=True,reason_codes=["ENCODING_BOM_CONFLICT"])
