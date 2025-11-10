package org.example;

import java.io.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

/**
 * Expression evaluator with simple "resources" mode:
 * - If no CLI args are provided, loads:
 *      resources/script.txt  (raw expression; quotes optional)
 *      resources/data.xml    (input XML)
 * - Otherwise, supports CLI like:
 *      java ExprEval -xml <path> "<expression>"
 *
 * Keeps all the semantics we discussed earlier.
 */
public class ExprEval {

    public static void main(String[] args) {
        try {
            Cli cli = Cli.parse(args);

            // --- Load expression ---
            String expr = cli.expression;
            if (expr == null) {
                expr = ResourceUtil.loadScriptFromResources();
                if (expr == null) {
                    throw new EvalException("USAGE: java ExprEval [-xml path] <expression>\n" +
                            "Or place resources/script.txt and resources/data.xml and run with no args.");
                }
            }
            expr = normalizeExpression(expr);
            String decoded = Html.decodeEntities(expr);

            // --- Load XML ---
            Document xmlDoc = null;
            if (cli.xmlPath != null) {
                xmlDoc = XmlUtil.parseXml(new File(cli.xmlPath));
            } else {
                xmlDoc = ResourceUtil.loadXmlFromResources();
                // If still null, variables resolve to empty string per spec (allowed)
            }

            Lexer lexer = new Lexer(decoded);
            Parser parser = new Parser(lexer);
            ExprNode ast = parser.parse();

            FunctionRegistry registry = new FunctionRegistry();
            StandardFunctions.registerAll(registry);

            Context ctx = new Context(xmlDoc, registry);

            Value result = ast.eval(ctx);
            System.out.println(ValuePrinter.print(result));
        } catch (EvalException e) {
            System.err.println(e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        }
    }

    private static String normalizeExpression(String expr) {
        String s = expr.trim();
        if (s.length() >= 2) {
            char a = s.charAt(0), b = s.charAt(s.length()-1);
            if ((a == '"' && b == '"') || (a == '\'' && b == '\'')) {
                return s.substring(1, s.length()-1);
            }
        }
        return s;
    }

    // -------------------- CLI --------------------
    private static final class Cli {
        final String xmlPath;
        final String expression;

        private Cli(String xmlPath, String expression) {
            this.xmlPath = xmlPath;
            this.expression = expression;
        }

        static Cli parse(String[] args) {
            String xmlPath = null;
            List<String> rest = new ArrayList<>();
            for (int i = 0; i < args.length; i++) {
                if ("-xml".equals(args[i]) && i + 1 < args.length) {
                    xmlPath = args[++i];
                } else {
                    rest.add(args[i]);
                }
            }
            if (rest.isEmpty()) {
                // No expression provided → will try resources mode
                return new Cli(xmlPath, null);
            }
            String expr = String.join(" ", rest);
            return new Cli(xmlPath, expr);
        }
    }

    // -------------------- Resource loader --------------------
    static final class ResourceUtil {
        private static final String[] SCRIPT_PATHS = {
                "/resources/script.txt", "/script.txt"
        };
        private static final String[] XML_PATHS = {
                "/resources/data.xml", "/data.xml"
        };

        static String loadScriptFromResources() {
            // 1) classpath
            for (String p : SCRIPT_PATHS) {
                try (InputStream in = ExprEval.class.getResourceAsStream(p)) {
                    if (in != null) return readAll(in);
                } catch (IOException ignored) {}
            }
            // 2) filesystem fallback
            for (String p : new String[]{"resources/script.txt", "script.txt"}) {
                File f = new File(p);
                if (f.isFile()) {
                    try (InputStream in = new FileInputStream(f)) {
                        return readAll(in);
                    } catch (IOException ignored) {}
                }
            }
            return null;
        }

        static Document loadXmlFromResources() {
            // 1) classpath
            for (String p : XML_PATHS) {
                try (InputStream in = ExprEval.class.getResourceAsStream(p)) {
                    if (in != null) return XmlUtil.parseXml(in);
                } catch (Exception ignored) {}
            }
            // 2) filesystem fallback
            for (String p : new String[]{"resources/data.xml", "data.xml"}) {
                File f = new File(p);
                if (f.isFile()) {
                    return XmlUtil.parseXml(f);
                }
            }
            return null;
        }

        private static String readAll(InputStream in) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int r;
            while ((r = in.read(buf)) != -1) bos.write(buf, 0, r);
            return bos.toString(StandardCharsets.UTF_8);
        }
    }

    // -------------------- HTML entity decoding --------------------
    static final class Html {
        static String decodeEntities(String s) {
            return s.replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&quot;", "\"")
                    .replace("&apos;", "'")
                    .replace("&amp;", "&");
        }
    }

    // -------------------- XML utility & resolver --------------------
    static final class XmlUtil {
        /** If true, A/@B falls back to A/B when attribute B is missing. */
        static final boolean LENIENT_ATTR_FALLBACK = true;

        static Document parseXml(File file) {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setNamespaceAware(false);
                dbf.setCoalescing(true);
                dbf.setIgnoringComments(true);
                dbf.setIgnoringElementContentWhitespace(false);
                DocumentBuilder db = dbf.newDocumentBuilder();
                return db.parse(file);
            } catch (Exception e) {
                throw new EvalException("PARSE_ERROR: cannot load XML - " + e.getMessage());
            }
        }

        static Document parseXml(InputStream in) {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setNamespaceAware(false);
                dbf.setCoalescing(true);
                dbf.setIgnoringComments(true);
                dbf.setIgnoringElementContentWhitespace(false);
                DocumentBuilder db = dbf.newDocumentBuilder();
                return db.parse(in);
            } catch (Exception e) {
                throw new EvalException("PARSE_ERROR: cannot load XML - " + e.getMessage());
            }
        }

        static String resolvePath(Document doc, String path) {
            if (doc == null || path == null || path.isEmpty()) return "";
            Element root = doc.getDocumentElement();
            String[] parts = path.split("/");
            if (parts.length == 0) return "";

            int i = 0;
            Node current = root;

            // If first segment equals root name, start at root; otherwise find direct child
            if (!root.getTagName().equals(parts[0])) {
                Node first = findFirstChildByName(current, parts[0]);
                if (first == null) return "";
                current = first;
                i = 1;
            } else {
                current = root;
                i = 1;
            }

            while (i < parts.length) {
                String seg = parts[i];

                // attribute step (with optional fallback)
                if (seg.startsWith("@")) {
                    String attr = seg.substring(1);
                    if (!(current instanceof Element)) return "";
                    Element el = (Element) current;

                    if (el.hasAttribute(attr)) {
                        String v = el.getAttribute(attr);
                        return v == null ? "" : v;
                    } else if (LENIENT_ATTR_FALLBACK) {
                        Node child = findFirstChildByName(current, attr);
                        if (child instanceof Element) return getStringValue((Element) child);
                        return "";
                    } else {
                        return "";
                    }
                } else if ("@".equals(seg)) { // support ".../@Attr"
                    if (i + 1 >= parts.length) return "";
                    String attr = parts[i + 1];
                    if (!(current instanceof Element)) return "";
                    Element el = (Element) current;

                    if (el.hasAttribute(attr)) {
                        String v = el.getAttribute(attr);
                        return v == null ? "" : v;
                    } else if (LENIENT_ATTR_FALLBACK) {
                        Node child = findFirstChildByName(current, attr);
                        if (child instanceof Element) return getStringValue((Element) child);
                        return "";
                    } else {
                        return "";
                    }
                }

                // element step
                Node next = findFirstChildByName(current, seg);
                if (next == null) return "";
                current = next;
                i++;
            }

            if (current instanceof Element) {
                return getStringValue((Element) current);
            }
            return "";
        }

        private static Node findFirstChildByName(Node node, String name) {
            for (Node c = node.getFirstChild(); c != null; c = c.getNextSibling()) {
                if (c instanceof Element) {
                    if (((Element) c).getTagName().equals(name)) return c;
                }
            }
            return null;
        }

        private static String getStringValue(Element e) {
            StringBuilder sb = new StringBuilder();
            Node n = e.getFirstChild();
            while (n != null) {
                if (n.getNodeType() == Node.TEXT_NODE || n.getNodeType() == Node.CDATA_SECTION_NODE) {
                    sb.append(n.getNodeValue());
                }
                n = n.getNextSibling();
            }
            return sb.toString();
        }
    }

    // -------------------- Value model --------------------
    enum Type { BOOLEAN, NUMERIC, STRING }
    interface Value { Type type(); }

    static final class BoolVal implements Value {
        final boolean v;
        BoolVal(boolean v) { this.v = v; }
        public Type type() { return Type.BOOLEAN; }
        public String toString() { return Boolean.toString(v); }
    }

    static final class NumVal implements Value {
        final BigDecimal v;
        final boolean isInteger;
        NumVal(BigDecimal v, boolean isInt) {
            this.v = v.stripTrailingZeros();
            this.isInteger = isInt && this.v.scale() <= 0;
        }
        static NumVal ofInt(long x) { return new NumVal(BigDecimal.valueOf(x), true); }
        static NumVal of(BigDecimal bd) { return new NumVal(bd, bd.scale() == 0); }
        public Type type() { return Type.NUMERIC; }
        public String toString() { return v.toPlainString(); }
    }

    static final class StrVal implements Value {
        final String v;
        StrVal(String v) { this.v = v == null ? "" : v; }
        public Type type() { return Type.STRING; }
        public String toString() { return v; }
    }

    static final class ValuePrinter {
        static String print(Value val) {
            if (val == null) return "";
            if (val instanceof StrVal) return ((StrVal) val).v;
            if (val instanceof BoolVal) return Boolean.toString(((BoolVal) val).v);
            if (val instanceof NumVal) return ((NumVal) val).v.toPlainString();
            return val.toString();
        }
    }

    // -------------------- Exceptions --------------------
    static final class EvalException extends RuntimeException {
        EvalException(String msg) { super(msg); }
    }

    // -------------------- Lexer --------------------
    enum TokType {
        IDENT, NUMBER, STRING, TRUE, FALSE,
        LPAREN, RPAREN, COMMA,
        PLUS, MINUS, STAR, SLASH, PERCENT,
        LT, LE, GT, GE, EQ, NE,
        ANDAND, OROR,
        AT,
        EOF
    }

    static final class Token {
        final TokType type;
        final String text;
        Token(TokType type, String text) { this.type = type; this.text = text; }
        public String toString() { return type + (text != null ? "(" + text + ")" : ""); }
    }

    static final class Lexer {
        private final String s;
        private int i = 0;

        Lexer(String s) { this.s = s; }

        Token next() {
            skipWs();
            if (i >= s.length()) return new Token(TokType.EOF, null);
            char c = s.charAt(i);

            if (isIdentStart(c)) {
                int start = i++;
                while (i < s.length() && isIdentPart(s.charAt(i))) i++;
                String ident = s.substring(start, i);
                if ("true".equals(ident)) return new Token(TokType.TRUE, ident);
                if ("false".equals(ident)) return new Token(TokType.FALSE, ident);
                return new Token(TokType.IDENT, ident);
            }

            if (Character.isDigit(c)) {
                int start = i++;
                while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
                if (i < s.length() && s.charAt(i) == '.') {
                    int j = i + 1;
                    if (j < s.length() && Character.isDigit(s.charAt(j))) {
                        i = j + 1;
                        while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
                    }
                }
                String num = s.substring(start, i);
                return new Token(TokType.NUMBER, num);
            }

            if (c == '\'' || c == '"') {
                char quote = c;
                i++;
                StringBuilder sb = new StringBuilder();
                while (i < s.length()) {
                    char ch = s.charAt(i++);
                    if (ch == quote) break;
                    if (ch == '\\') {
                        if (i >= s.length()) throw new EvalException("PARSE_ERROR: unfinished escape sequence");
                        char e = s.charAt(i++);
                        switch (e) {
                            case '\\': sb.append('\\'); break;
                            case '\'': sb.append('\''); break;
                            case '"': sb.append('"'); break;
                            case 'n': sb.append('\n'); break;
                            case 'r': sb.append('\r'); break;
                            case 't': sb.append('\t'); break;
                            case 'u': {
                                if (i + 3 >= s.length()) throw new EvalException("PARSE_ERROR: bad \\u escape");
                                String hex = s.substring(i, i + 4);
                                i += 4;
                                try {
                                    char uc = (char) Integer.parseInt(hex, 16);
                                    sb.append(uc);
                                } catch (NumberFormatException ex) {
                                    throw new EvalException("PARSE_ERROR: bad \\u escape");
                                }
                                break;
                            }
                            default: sb.append(e); break;
                        }
                    } else {
                        sb.append(ch);
                    }
                }
                return new Token(TokType.STRING, sb.toString());
            }

            if (i + 1 < s.length()) {
                String two = s.substring(i, i + 2);
                switch (two) {
                    case "&&": i += 2; return new Token(TokType.ANDAND, "&&");
                    case "||": i += 2; return new Token(TokType.OROR, "||");
                    case "==": i += 2; return new Token(TokType.EQ, "==");
                    case "!=": i += 2; return new Token(TokType.NE, "!=");
                    case "<=": i += 2; return new Token(TokType.LE, "<=");
                    case ">=": i += 2; return new Token(TokType.GE, ">=");
                }
            }

            i++;
            switch (c) {
                case '(' : return new Token(TokType.LPAREN, "(");
                case ')' : return new Token(TokType.RPAREN, ")");
                case ',' : return new Token(TokType.COMMA, ",");
                case '+' : return new Token(TokType.PLUS, "+");
                case '-' : return new Token(TokType.MINUS, "-");
                case '*' : return new Token(TokType.STAR, "*");
                case '/' : return new Token(TokType.SLASH, "/");
                case '%' : return new Token(TokType.PERCENT, "%");
                case '<' : return new Token(TokType.LT, "<");
                case '>' : return new Token(TokType.GT, ">");
                case '@' : return new Token(TokType.AT, "@");
                case '&' :
                case '|' :
                    throw new EvalException("PARSE_ERROR: single '&' or '|' is not allowed");
                default:
                    throw new EvalException("PARSE_ERROR: unexpected character '" + c + "'");
            }
        }

        private void skipWs() {
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') i++;
                else break;
            }
        }

        private static boolean isIdentStart(char c) { return Character.isLetter(c) || c == '_'; }
        private static boolean isIdentPart(char c) { return Character.isLetterOrDigit(c) || c == '_'; }
    }

    // -------------------- Parser / AST --------------------
    interface ExprNode { Value eval(Context ctx); }

    static final class Parser {
        private final Lexer lexer;
        private Token la;

        Parser(Lexer lexer) {
            this.lexer = lexer;
            this.la = lexer.next();
        }

        private Token eat(TokType t) {
            if (la.type != t) throw new EvalException("PARSE_ERROR: expected " + t + " but got " + la.type);
            Token cur = la; la = lexer.next(); return cur;
        }
        private boolean look(TokType t) { return la.type == t; }

        ExprNode parse() {
            ExprNode n = parseExpr();
            if (la.type != TokType.EOF) throw new EvalException("PARSE_ERROR: unexpected token " + la);
            return n;
        }
        private ExprNode parseExpr() { return parseOr(); }

        private ExprNode parseOr() {
            ExprNode left = parseAnd();
            while (look(TokType.OROR)) { eat(TokType.OROR); ExprNode right = parseAnd(); left = new BinOp("||", left, right); }
            return left;
        }
        private ExprNode parseAnd() {
            ExprNode left = parseRel();
            while (look(TokType.ANDAND)) { eat(TokType.ANDAND); ExprNode right = parseRel(); left = new BinOp("&&", left, right); }
            return left;
        }
        private ExprNode parseRel() {
            ExprNode left = parseAdd();
            while (true) {
                if (look(TokType.EQ)) { eat(TokType.EQ); left = new BinOp("==", left, parseAdd()); }
                else if (look(TokType.NE)) { eat(TokType.NE); left = new BinOp("!=", left, parseAdd()); }
                else if (look(TokType.LT)) { eat(TokType.LT); left = new BinOp("<", left, parseAdd()); }
                else if (look(TokType.LE)) { eat(TokType.LE); left = new BinOp("<=", left, parseAdd()); }
                else if (look(TokType.GT)) { eat(TokType.GT); left = new BinOp(">", left, parseAdd()); }
                else if (look(TokType.GE)) { eat(TokType.GE); left = new BinOp(">=", left, parseAdd()); }
                else break;
            }
            return left;
        }
        private ExprNode parseAdd() {
            ExprNode left = parseMul();
            while (true) {
                if (look(TokType.PLUS)) { eat(TokType.PLUS); left = new BinOp("+", left, parseMul()); }
                else if (look(TokType.MINUS)) { eat(TokType.MINUS); left = new BinOp("-", left, parseMul()); }
                else break;
            }
            return left;
        }
        private ExprNode parseMul() {
            ExprNode left = parseUnary();
            while (true) {
                if (look(TokType.STAR)) { eat(TokType.STAR); left = new BinOp("*", left, parseUnary()); }
                else if (look(TokType.SLASH)) { eat(TokType.SLASH); left = new BinOp("/", left, parseUnary()); }
                else if (look(TokType.PERCENT)) { eat(TokType.PERCENT); left = new BinOp("%", left, parseUnary()); }
                else break;
            }
            return left;
        }
        private ExprNode parseUnary() {
            if (look(TokType.MINUS)) { eat(TokType.MINUS); return new UnaryOp("-", parseUnary()); }
            return parsePrimary();
        }
        private ExprNode parsePrimary() {
            switch (la.type) {
                case TRUE: eat(TokType.TRUE); return new BoolLit(true);
                case FALSE: eat(TokType.FALSE); return new BoolLit(false);
                case NUMBER: return new NumLit(new BigDecimal(eat(TokType.NUMBER).text));
                case STRING: return new StrLit(eat(TokType.STRING).text);
                case IDENT: {
                    String ident = eat(TokType.IDENT).text;
                    if (look(TokType.LPAREN)) {
                        eat(TokType.LPAREN);
                        List<ExprNode> args = new ArrayList<>();
                        if (!look(TokType.RPAREN)) {
                            args.add(parseExpr());
                            while (look(TokType.COMMA)) { eat(TokType.COMMA); args.add(parseExpr()); }
                        }
                        eat(TokType.RPAREN);
                        return new FuncCall(ident, args);
                    } else {
                        List<String> segments = new ArrayList<>();
                        segments.add(ident);
                        while (look(TokType.SLASH)) {
                            eat(TokType.SLASH);
                            if (look(TokType.AT)) {
                                eat(TokType.AT);
                                String attr = eat(TokType.IDENT).text;
                                segments.add("@" + attr);
                                break;
                            } else {
                                segments.add(eat(TokType.IDENT).text);
                            }
                        }
                        return new VarRef(String.join("/", segments));
                    }
                }
                case LPAREN: {
                    eat(TokType.LPAREN);
                    ExprNode n = parseExpr();
                    eat(TokType.RPAREN);
                    return n;
                }
                default: throw new EvalException("PARSE_ERROR: unexpected token " + la);
            }
        }
    }

    // -------------------- AST nodes --------------------
    static final class BoolLit implements ExprNode {
        final boolean v; BoolLit(boolean v) { this.v = v; }
        public Value eval(Context ctx) { return new BoolVal(v); }
    }
    static final class NumLit implements ExprNode {
        final BigDecimal v; NumLit(BigDecimal v) { this.v = v; }
        public Value eval(Context ctx) { return new NumVal(v, v.scale() == 0); }
    }
    static final class StrLit implements ExprNode {
        final String v; StrLit(String v) { this.v = v; }
        public Value eval(Context ctx) { return new StrVal(v); }
    }
    static final class VarRef implements ExprNode {
        final String path; VarRef(String path) { this.path = path; }
        public Value eval(Context ctx) {
            String val = XmlUtil.resolvePath(ctx.xml, path);
            return new StrVal(val == null ? "" : val);
        }
    }
    static final class UnaryOp implements ExprNode {
        final String op; final ExprNode expr;
        UnaryOp(String op, ExprNode expr) { this.op = op; this.expr = expr; }
        public Value eval(Context ctx) {
            Value v = expr.eval(ctx);
            if ("-".equals(op)) {
                if (!(v instanceof NumVal)) throw new EvalException("TYPE_ERROR: unary '-' expects numeric");
                NumVal n = (NumVal) v; return new NumVal(n.v.negate(), n.isInteger);
            }
            throw new EvalException("PARSE_ERROR: unknown unary op " + op);
        }
    }
    static final class BinOp implements ExprNode {
        final String op; final ExprNode l, r;
        BinOp(String op, ExprNode l, ExprNode r) { this.op = op; this.l = l; this.r = r; }
        private static final int DIV_SCALE = 10;
        public Value eval(Context ctx) {
            Value lv = l.eval(ctx), rv = r.eval(ctx);
            switch (op) {
                case "+":
                    if (lv instanceof NumVal && rv instanceof NumVal) {
                        NumVal a = (NumVal) lv, b = (NumVal) rv;
                        return new NumVal(a.v.add(b.v), a.isInteger && b.isInteger);
                    } else if (lv instanceof StrVal && rv instanceof StrVal) {
                        return new StrVal(((StrVal) lv).v + ((StrVal) rv).v);
                    }
                    throw new EvalException("TYPE_ERROR: '+' expects both numeric or both string");
                case "-":
                    ensureNumeric(lv, rv, "'-'");
                    return new NumVal(((NumVal) lv).v.subtract(((NumVal) rv).v),
                            ((NumVal) lv).isInteger && ((NumVal) rv).isInteger);
                case "*":
                    ensureNumeric(lv, rv, "'*'");
                    return new NumVal(((NumVal) lv).v.multiply(((NumVal) rv).v),
                            ((NumVal) lv).isInteger && ((NumVal) rv).isInteger);
                case "/":
                    ensureNumeric(lv, rv, "'/'");
                    BigDecimal denom = ((NumVal) rv).v;
                    if (denom.compareTo(BigDecimal.ZERO) == 0) throw new EvalException("DIVIDE_BY_ZERO");
                    BigDecimal q = ((NumVal) lv).v.divide(denom, DIV_SCALE, RoundingMode.HALF_UP);
                    return new NumVal(q, false);
                case "%":
                    if (!(lv instanceof NumVal) || !(rv instanceof NumVal))
                        throw new EvalException("TYPE_ERROR: '%' expects numeric");
                    NumVal a = (NumVal) lv, b = (NumVal) rv;
                    if (!a.isInteger || !b.isInteger)
                        throw new EvalException("TYPE_ERROR: '%' expects integer operands");
                    BigInteger ai = a.v.toBigIntegerExact(), bi = b.v.toBigIntegerExact();
                    if (bi.equals(BigInteger.ZERO)) throw new EvalException("DIVIDE_BY_ZERO");
                    return new NumVal(new BigDecimal(ai.mod(bi)), true);
                case "==": return compareEq(lv, rv, true);
                case "!=": return compareEq(lv, rv, false);
                case "<":  return compareOrder("<",  lv, rv);
                case "<=": return compareOrder("<=", lv, rv);
                case ">":  return compareOrder(">",  lv, rv);
                case ">=": return compareOrder(">=", lv, rv);
                case "&&":
                case "||":
                    if (!(lv instanceof BoolVal) || !(rv instanceof BoolVal))
                        throw new EvalException("TYPE_ERROR: logical operators expect boolean operands");
                    boolean lb = ((BoolVal) lv).v, rb = ((BoolVal) rv).v;
                    return new BoolVal("&&".equals(op) ? (lb && rb) : (lb || rb));
                default: throw new EvalException("PARSE_ERROR: unknown operator " + op);
            }
        }
        private static void ensureNumeric(Value lv, Value rv, String op) {
            if (!(lv instanceof NumVal) || !(rv instanceof NumVal))
                throw new EvalException("TYPE_ERROR: " + op + " expects numeric operands");
        }
        private static Value compareEq(Value lv, Value rv, boolean isEq) {
            if (lv instanceof NumVal && rv instanceof NumVal)
                return new BoolVal(((NumVal) lv).v.compareTo(((NumVal) rv).v) == 0 ? isEq : !isEq);
            if (lv instanceof StrVal && rv instanceof StrVal)
                return new BoolVal(Objects.equals(((StrVal) lv).v, ((StrVal) rv).v) ? isEq : !isEq);
            if (lv instanceof BoolVal && rv instanceof BoolVal)
                return new BoolVal((((BoolVal) lv).v == ((BoolVal) rv).v) ? isEq : !isEq);
            throw new EvalException("TYPE_ERROR: equality requires same types");
        }
        private static Value compareOrder(String op, Value lv, Value rv) {
            if (lv instanceof NumVal && rv instanceof NumVal) {
                int c = ((NumVal) lv).v.compareTo(((NumVal) rv).v);
                return new BoolVal(cmp(op, c));
            }
            if (lv instanceof StrVal && rv instanceof StrVal) {
                int c = ((StrVal) lv).v.compareTo(((StrVal) rv).v);
                return new BoolVal(cmp(op, c));
            }
            throw new EvalException("TYPE_ERROR: ordering requires both numeric or both string");
        }
        private static boolean cmp(String op, int comp) {
            switch (op) {
                case "<": return comp < 0;
                case "<=": return comp <= 0;
                case ">": return comp > 0;
                case ">=": return comp >= 0;
            }
            throw new IllegalStateException(op);
        }
    }

    // -------------------- Functions --------------------
    interface Function { Value invoke(Context ctx, List<Value> args); }

    static final class FunctionRegistry {
        private final Map<String, Function> map = new HashMap<>();
        void register(String name, Function f) { map.put(name.toLowerCase(Locale.ROOT), f); }
        Function resolve(String name) {
            Function f = map.get(name.toLowerCase(Locale.ROOT));
            if (f == null) throw new EvalException("UNKNOWN_FUNCTION: '" + name + "'");
            return f;
        }
    }

    static final class FuncCall implements ExprNode {
        final String name; final List<ExprNode> args;
        FuncCall(String name, List<ExprNode> args) { this.name = name; this.args = args; }
        public Value eval(Context ctx) {
            Function f = ctx.functions.resolve(name);
            List<Value> av = new ArrayList<>(args.size());
            for (ExprNode n : args) av.add(n.eval(ctx));
            return f.invoke(ctx, av);
        }
    }

    static final class Context {
        final Document xml; final FunctionRegistry functions;
        Context(Document xml, FunctionRegistry functions) { this.xml = xml; this.functions = functions; }
    }

    static final class StandardFunctions {
        static void registerAll(FunctionRegistry r) {
            r.register("If", (ctx, a) -> {
                ensureArity("If", a, 3);
                BoolVal cond = asBool("If", 1, a.get(0));
                return cond.v ? a.get(1) : a.get(2);
            });

            r.register("Concat", (ctx, a) -> {
                if (a.isEmpty()) throw new EvalException("ARITY_MISMATCH: 'Concat' expects >=1 args");
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < a.size(); i++) sb.append(asStr("Concat", i+1, a.get(i)));
                return new StrVal(sb.toString());
            });

            r.register("UpperCase", (ctx, a) -> { ensureArity("UpperCase", a, 1); return new StrVal(asStr("UpperCase", 1, a.get(0)).toUpperCase(Locale.ROOT)); });
            r.register("LowerCase", (ctx, a) -> { ensureArity("LowerCase", a, 1); return new StrVal(asStr("LowerCase", 1, a.get(0)).toLowerCase(Locale.ROOT)); });
            r.register("TitleCase", (ctx, a) -> { ensureArity("TitleCase", a, 1); return new StrVal(toTitleCase(asStr("TitleCase", 1, a.get(0)))); });

            r.register("Replace", (ctx, a) -> { ensureArity("Replace", a, 3); return new StrVal(asStr("Replace", 1, a.get(0)).replace(asStr("Replace", 2, a.get(1)), asStr("Replace", 3, a.get(2)))); });
            r.register("Trim", (ctx, a) -> { ensureArity("Trim", a, 2); return new StrVal(trimCustom(asStr("Trim", 1, a.get(0)), asStr("Trim", 2, a.get(1)))); });
            r.register("Length", (ctx, a) -> { ensureArity("Length", a, 1); return NumVal.ofInt(asStr("Length", 1, a.get(0)).length()); });
            r.register("Insert", (ctx, a) -> {
                ensureArity("Insert", a, 3);
                String input = asStr("Insert", 1, a.get(0)), ins = asStr("Insert", 2, a.get(1));
                int at = asInt("Insert", 3, a.get(2));
                if (at < 0 || at > input.length()) throw new EvalException("RANGE_ERROR: Insert index out of range");
                return new StrVal(input.substring(0, at) + ins + input.substring(at));
            });

            r.register("IndexOf", (ctx, a) -> {
                if (a.size() != 2 && a.size() != 3) throw new EvalException("ARITY_MISMATCH: 'IndexOf' expects 2 or 3 args");
                String input = asStr("IndexOf", 1, a.get(0)), search = asStr("IndexOf", 2, a.get(1));
                int start = (a.size() == 3) ? asInt("IndexOf", 3, a.get(2)) : 0;
                if (start < 0 || start > input.length()) return NumVal.ofInt(-1);
                return NumVal.ofInt(input.indexOf(search, start));
            });

            r.register("LastIndexOf", (ctx, a) -> {
                if (a.size() != 2 && a.size() != 3) throw new EvalException("ARITY_MISMATCH: 'LastIndexOf' expects 2 or 3 args");
                String input = asStr("LastIndexOf", 1, a.get(0)), search = asStr("LastIndexOf", 2, a.get(1));
                int start = input.length();
                if (a.size() == 3) { start = asInt("LastIndexOf", 3, a.get(2)); if (start > input.length()) start = input.length(); }
                return NumVal.ofInt(input.lastIndexOf(search, start));
            });

            r.register("SubString", (ctx, a) -> {
                if (a.size() != 2 && a.size() != 3) throw new EvalException("ARITY_MISMATCH: 'SubString' expects 2 or 3 args");
                String input = asStr("SubString", 1, a.get(0));
                int start = asInt("SubString", 2, a.get(1));
                if (start < 0 || start > input.length()) throw new EvalException("RANGE_ERROR: SubString start out of range");
                if (a.size() == 2) return new StrVal(input.substring(start));
                int len = asInt("SubString", 3, a.get(2));
                if (len < 0) throw new EvalException("RANGE_ERROR: SubString length negative");
                int end = start + len;
                if (end > input.length()) throw new EvalException("RANGE_ERROR: SubString end out of range");
                return new StrVal(input.substring(start, end));
            });

            r.register("SubStringBefore", (ctx, a) -> { ensureArity("SubStringBefore", a, 2);
                String s = asStr("SubStringBefore", 1, a.get(0)), t = asStr("SubStringBefore", 2, a.get(1));
                int i = s.indexOf(t); return new StrVal(i < 0 ? "" : s.substring(0, i)); });

            r.register("SubStringAfter", (ctx, a) -> { ensureArity("SubStringAfter", a, 2);
                String s = asStr("SubStringAfter", 1, a.get(0)), t = asStr("SubStringAfter", 2, a.get(1));
                int i = s.indexOf(t); return new StrVal(i < 0 ? "" : s.substring(i + t.length())); });

            r.register("Add", (ctx, a) -> { ensureArity("Add", a, 2);
                NumVal x = asNum("Add", 1, a.get(0)), y = asNum("Add", 2, a.get(1));
                return new NumVal(x.v.add(y.v), x.isInteger && y.isInteger); });

            r.register("Subtract", (ctx, a) -> { ensureArity("Subtract", a, 2);
                NumVal x = asNum("Subtract", 1, a.get(0)), y = asNum("Subtract", 2, a.get(1));
                return new NumVal(x.v.subtract(y.v), x.isInteger && y.isInteger); });

            r.register("Multiply", (ctx, a) -> { ensureArity("Multiply", a, 2);
                NumVal x = asNum("Multiply", 1, a.get(0)), y = asNum("Multiply", 2, a.get(1));
                return new NumVal(x.v.multiply(y.v), x.isInteger && y.isInteger); });

            r.register("Divide", (ctx, a) -> { ensureArity("Divide", a, 2);
                NumVal x = asNum("Divide", 1, a.get(0)), y = asNum("Divide", 2, a.get(1));
                if (y.v.compareTo(BigDecimal.ZERO) == 0) throw new EvalException("DIVIDE_BY_ZERO");
                return new NumVal(x.v.divide(y.v, 10, RoundingMode.HALF_UP), false); });

            r.register("Mod", (ctx, a) -> { ensureArity("Mod", a, 2);
                NumVal x = asNum("Mod", 1, a.get(0)), y = asNum("Mod", 2, a.get(1));
                if (!x.isInteger || !y.isInteger) throw new EvalException("TYPE_ERROR: Mod expects integer operands");
                BigInteger xi = x.v.toBigIntegerExact(), yi = y.v.toBigIntegerExact();
                if (yi.equals(BigInteger.ZERO)) throw new EvalException("DIVIDE_BY_ZERO");
                return new NumVal(new BigDecimal(xi.mod(yi)), true); });

            r.register("Abs", (ctx, a) -> { ensureArity("Abs", a, 1); NumVal x = asNum("Abs", 1, a.get(0)); return new NumVal(x.v.abs(), x.isInteger); });
            r.register("Floor", (ctx, a) -> { ensureArity("Floor", a, 1); NumVal x = asNum("Floor", 1, a.get(0)); return new NumVal(x.v.setScale(0, RoundingMode.FLOOR), true); });
            r.register("Ceil", (ctx, a) -> { ensureArity("Ceil", a, 1); NumVal x = asNum("Ceil", 1, a.get(0)); return new NumVal(x.v.setScale(0, RoundingMode.CEILING), true); });
            r.register("Round", (ctx, a) -> { ensureArity("Round", a, 1); NumVal x = asNum("Round", 1, a.get(0)); return new NumVal(x.v.setScale(0, RoundingMode.HALF_UP), true); });

            r.register("MaskNumber", (ctx, a) -> { ensureArity("MaskNumber", a, 2);
                NumVal x = asNum("MaskNumber", 1, a.get(0)); String pattern = asStr("MaskNumber", 2, a.get(1));
                java.text.DecimalFormat df = new java.text.DecimalFormat(pattern, java.text.DecimalFormatSymbols.getInstance(java.util.Locale.US));
                return new StrVal(df.format(x.v)); });

            r.register("MaskPhoneNumber", (ctx, a) -> { ensureArity("MaskPhoneNumber", a, 2);
                String digits = onlyDigits(asStr("MaskPhoneNumber", 1, a.get(0))); String mask = asStr("MaskPhoneNumber", 2, a.get(1));
                StringBuilder out = new StringBuilder(); int di = 0;
                for (int i = 0; i < mask.length(); i++) { char mc = mask.charAt(i);
                    out.append(mc == '#' ? (di < digits.length() ? digits.charAt(di++) : '#') : mc); }
                return new StrVal(out.toString()); });

            r.register("MaskDateTime", (ctx, a) -> { ensureArity("MaskDateTime", a, 2);
                LocalDateTime dt = parseToDateTime(asStr("MaskDateTime", 1, a.get(0)));
                DateTimeFormatter out = DateTimeFormatter.ofPattern(asStr("MaskDateTime", 2, a.get(1)));
                return new StrVal(dt.format(out)); });

            r.register("NumberToWords", (ctx, a) -> { ensureArity("NumberToWords", a, 1);
                NumVal n = asNum("NumberToWords", 1, a.get(0)); if (!n.isInteger) throw new EvalException("TYPE_ERROR: NumberToWords expects integer");
                return new StrVal(englishWords(n.v.toBigIntegerExact())); });

            r.register("ToDateTime", (ctx, a) -> {
                ensureArity("ToDateTime", a, 2);
                String src = asStr("ToDateTime", 1, a.get(0)), fmt = asStr("ToDateTime", 2, a.get(1));
                DateTimeFormatter f = DateTimeFormatter.ofPattern(fmt);
                TemporalAccessor ta;
                try { ta = f.parse(src); } catch (Exception e) { throw new EvalException("DOMAIN_ERROR: invalid date format"); }
                LocalDateTime dt;
                try { dt = LocalDateTime.from(ta); } catch (Exception e) { dt = LocalDate.from(ta).atStartOfDay(); }
                return new StrVal(dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")));
            });

            r.register("AddInterval", (ctx, a) -> { ensureArity("AddInterval", a, 3);
                LocalDateTime dt = parseToDateTime(asStr("AddInterval", 1, a.get(0)));
                NumVal iv = asNum("AddInterval", 2, a.get(1)); String unit = asStr("AddInterval", 3, a.get(2));
                return new StrVal(applyInterval(dt, iv, unit, true)); });

            r.register("SubtractInterval", (ctx, a) -> { ensureArity("SubtractInterval", a, 3);
                LocalDateTime dt = parseToDateTime(asStr("SubtractInterval", 1, a.get(0)));
                NumVal iv = asNum("SubtractInterval", 2, a.get(1)); String unit = asStr("SubtractInterval", 3, a.get(2));
                return new StrVal(applyInterval(dt, iv, unit, false)); });

            r.register("Duration", (ctx, a) -> { ensureArity("Duration", a, 3);
                LocalDateTime a1 = parseToDateTime(asStr("Duration", 1, a.get(0)));
                LocalDateTime a2 = parseToDateTime(asStr("Duration", 2, a.get(1)));
                return NumVal.of(durationBetween(a1, a2, asStr("Duration", 3, a.get(2)))); });
        }

        // helpers
        static void ensureArity(String name, List<Value> args, int expected) {
            if (args.size() != expected) throw new EvalException("ARITY_MISMATCH: '" + name + "' expects " + expected + " args");
        }
        static String asStr(String fn, int pos, Value v) {
            if (!(v instanceof StrVal)) throw new EvalException("TYPE_ERROR: argument #" + pos + " to '" + fn + "' must be String");
            return ((StrVal) v).v;
        }
        static NumVal asNum(String fn, int pos, Value v) {
            if (!(v instanceof NumVal)) throw new EvalException("TYPE_ERROR: argument #" + pos + " to '" + fn + "' must be Numeric");
            return (NumVal) v;
        }
        static BoolVal asBool(String fn, int pos, Value v) {
            if (!(v instanceof BoolVal)) throw new EvalException("TYPE_ERROR: argument #" + pos + " to '" + fn + "' must be Boolean");
            return (BoolVal) v;
        }
        static int asInt(String fn, int pos, Value v) {
            NumVal n = asNum(fn, pos, v);
            try { return n.v.intValueExact(); }
            catch (ArithmeticException e) { throw new EvalException("TYPE_ERROR: argument #" + pos + " to '" + fn + "' must be integer"); }
        }

        static String toTitleCase(String s) {
            if (s == null || s.isEmpty()) return s;
            StringBuilder out = new StringBuilder(s.length());
            boolean newWord = true;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (Character.isLetterOrDigit(c)) {
                    out.append(newWord ? Character.toTitleCase(c) : Character.toLowerCase(c));
                    newWord = false;
                } else { out.append(c); newWord = true; }
            }
            return out.toString();
        }

        static String trimCustom(String input, String trimChars) {
            if (trimChars.isEmpty()) return input.trim();
            Set<Character> set = new HashSet<>();
            for (char c : trimChars.toCharArray()) set.add(c);
            int start = 0, end = input.length();
            while (start < end && set.contains(input.charAt(start))) start++;
            while (end > start && set.contains(input.charAt(end - 1))) end--;
            return input.substring(start, end);
        }
        static String onlyDigits(String s) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < s.length(); i++) { char c = s.charAt(i); if (c >= '0' && c <= '9') sb.append(c); }
            return sb.toString();
        }

        static LocalDateTime parseToDateTime(String input) {
            List<DateTimeFormatter> fmts = Arrays.asList(
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                    DateTimeFormatter.ISO_LOCAL_DATE
            );
            for (DateTimeFormatter f : fmts) {
                try {
                    TemporalAccessor ta = f.parse(input);
                    try { return LocalDateTime.from(ta); }
                    catch (Exception e) { return LocalDate.from(ta).atStartOfDay(); }
                } catch (DateTimeParseException ignored) {}
            }
            throw new EvalException("DOMAIN_ERROR: invalid date value");
        }

        static String applyInterval(LocalDateTime dt, NumVal iv, String unit, boolean add) {
            int amount;
            try { amount = iv.v.intValueExact(); }
            catch (ArithmeticException e) { throw new EvalException("TYPE_ERROR: interval must be integer"); }
            if (!add) amount = -amount;
            switch (unit.toUpperCase(Locale.ROOT)) {
                case "YEARS": case "YEAR":
                    return dt.plusYears(amount).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                case "MONTHS": case "MONTH":
                    return dt.plusMonths(amount).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                case "DAYS": case "DAY":
                    return dt.plusDays(amount).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                case "HOURS": case "HOUR":
                    return dt.plusHours(amount).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                case "MINUTES": case "MINUTE":
                    return dt.plusMinutes(amount).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                case "SECONDS": case "SECOND":
                    return dt.plusSeconds(amount).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                default: throw new EvalException("DOMAIN_ERROR: unknown DateField '" + unit + "'");
            }
        }

        static BigDecimal durationBetween(LocalDateTime a, LocalDateTime b, String unit) {
            switch (unit.toUpperCase(Locale.ROOT)) {
                case "YEARS": case "YEAR":  return BigDecimal.valueOf(java.time.Period.between(a.toLocalDate(), b.toLocalDate()).getYears());
                case "MONTHS": case "MONTH": {
                    java.time.Period p = java.time.Period.between(a.toLocalDate(), b.toLocalDate());
                    long months = p.getYears() * 12L + p.getMonths();
                    return BigDecimal.valueOf(months);
                }
                case "DAYS": case "DAY":    return BigDecimal.valueOf(ChronoUnit.DAYS.between(a, b));
                case "HOURS": case "HOUR":  return BigDecimal.valueOf(ChronoUnit.HOURS.between(a, b));
                case "MINUTES": case "MINUTE": return BigDecimal.valueOf(ChronoUnit.MINUTES.between(a, b));
                case "SECONDS": case "SECOND": return BigDecimal.valueOf(ChronoUnit.SECONDS.between(a, b));
                default: throw new EvalException("DOMAIN_ERROR: unknown DateField '" + unit + "'");
            }
        }

        // number to words (English, up to trillions)
        static final String[] smalls = {"zero","one","two","three","four","five","six","seven","eight","nine",
                "ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen"};
        static final String[] tens = {"","","twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"};
        static final String[] thousands = {"","thousand","million","billion","trillion"};

        static String englishWords(BigInteger n) {
            if (n.equals(BigInteger.ZERO)) return "zero";
            String sign = "";
            if (n.signum() < 0) { sign = "minus "; n = n.negate(); }
            List<String> parts = new ArrayList<>();
            int idx = 0;
            while (n.compareTo(BigInteger.ZERO) > 0 && idx < thousands.length) {
                BigInteger[] qr = n.divideAndRemainder(BigInteger.valueOf(1000));
                int chunk = qr[1].intValue();
                if (chunk != 0) {
                    String w = three(chunk);
                    if (!thousands[idx].isEmpty()) w += " " + thousands[idx];
                    parts.add(w);
                }
                n = qr[0]; idx++;
            }
            Collections.reverse(parts);
            return sign + String.join(" ", parts).replaceAll(" +", " ").trim();
        }
        static String three(int n) {
            StringBuilder sb = new StringBuilder();
            if (n >= 100) { sb.append(smalls[n/100]).append(" hundred"); n %= 100; if (n != 0) sb.append(" "); }
            if (n >= 20) { sb.append(tens[n/10]); if (n % 10 != 0) sb.append("-").append(smalls[n%10]); }
            else if (n > 0) { sb.append(smalls[n]); }
            return sb.toString();
        }
    }
}