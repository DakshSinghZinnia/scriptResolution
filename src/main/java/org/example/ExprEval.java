package org.example;

import java.io.*;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ExprEval {

    // =========================================================
    // Configuration
    // =========================================================
    private static final String INPUT_DIR = "input";
    private static final String INTERMEDIATE_DIR = "intermediate";
    private static final String OUTPUT_DIR = "output";
    private static final String INPUT_FILE = "input.json";
    private static final String OUTPUT_FILE = "output.json";

    // =========================================================
    // Entry
    // =========================================================
    public static void main(String[] args) {
        try {
            // Initialize function registry
            FunctionRegistry registry = new FunctionRegistry();
            StandardFunctions.registerAll(registry);

            // Define conversion nodes - each node is a List of (path, script) pairs
            // Using List instead of Map to allow multiple scripts for the same path
            List<List<Map.Entry<String, String>>> conversionNodes = createConversionNodes();

            // Load input JSON
            ObjectNode inputJson = JsonUtil.loadInputJson();
            System.out.println("Loaded input.json");

            // Initialize output JSON as a copy of input JSON
            // Output JSON holds the snapshot from the previous node (for cross-variable references)
            ObjectNode outputJson = JsonUtil.deepCopy(inputJson);

            // Clear intermediate directory
            JsonUtil.clearIntermediateDir();

            // Process each conversion node
            for (int nodeIndex = 0; nodeIndex < conversionNodes.size(); nodeIndex++) {
                List<Map.Entry<String, String>> node = conversionNodes.get(nodeIndex);
                System.out.println("\n=== Processing Node " + (nodeIndex + 1) + " ===");

                // Create intermediate JSON as copy of output JSON at start of each node
                // Intermediate JSON is updated after each script (for self-references within same node)
                ObjectNode intermediateJson = JsonUtil.deepCopy(outputJson);
                String intermediateFile = "node_" + (nodeIndex + 1) + ".json";

                for (Map.Entry<String, String> entry : node) {
                    String targetPath = entry.getKey();
                    String script = entry.getValue();

                    System.out.println("  Path: " + targetPath);
                    System.out.println("  Script: " + script);

                    String resultStr;
                    
                    // Handle empty scripts - just set to empty string without parsing
                    if (script == null || script.trim().isEmpty()) {
                        resultStr = "";
                    } else {
                        // Evaluate the script with context containing:
                        // - outputJson: for cross-variable references (previous node's snapshot)
                        // - intermediateJson: for self-references (evolving state within current node)
                        // - targetPath: to determine if a reference is self or cross
                        String scriptDecoded = Html.decodeEntities(script);
                        Lexer lexer = new Lexer(scriptDecoded);
                        Parser parser = new Parser(lexer);
                        ExprNode ast = parser.parse();
                        Context ctx = new Context(outputJson, intermediateJson, targetPath, registry);
                        Value result = ast.eval(ctx);
                        resultStr = ValuePrinter.print(result);
                    }

                    System.out.println("  Result: " + resultStr);

                    // Update intermediate JSON after each script
                    JsonUtil.setValueAtPath(intermediateJson, targetPath, resultStr);

                    // Save intermediate JSON after each script execution
                    JsonUtil.saveIntermediateJson(intermediateJson, intermediateFile);
                    System.out.println("  Updated intermediate: " + intermediateFile);
                }

                // After all scripts in node complete, update output JSON from intermediate
                outputJson = intermediateJson;
                System.out.println("Node " + (nodeIndex + 1) + " complete. Output JSON updated.");
            }

            // Save final output
            JsonUtil.saveOutputJson(outputJson);
            System.out.println("\n=== Saved final output.json ===");

        } catch (EvalException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        }
    }

    /**
     * Define conversion nodes here.
     * Each node is a List of (path, script) entries to allow multiple scripts for the same path.
     * Key = target JSON path (e.g., "LetterData/M_Amount")
     * Value = script to evaluate (result will be stored at the path)
     */
    private static List<List<Map.Entry<String, String>>> createConversionNodes() {
        List<List<Map.Entry<String, String>>> nodes = new ArrayList<>();

        // === Node 1: Basic transformations ===
        List<Map.Entry<String, String>> node1 = new ArrayList<>();
        node1.add(entry("PlanCode_Lookup/ProductType", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 27, \"\")"));
        node1.add(entry("LetterData/People_Annuitant_FullName", "Concat(LetterData/People_Annuitant_FirstName,' ',LetterData/People_Annuitant_LastName)"));
        node1.add(entry("LetterData/Owner_FullName", "Concat(LetterData/Owner_FirstName,' ',LetterData/Owner_LastName)"));
        node1.add(entry("LetterData/M_Name", "if((LetterData/DocInfo/DocName == 'Annuitization Letter_MM' || LetterData/DocInfo/DocName == 'Attempt to Locate' || LetterData/DocInfo/DocName == 'Death Initial Notification_MM' || LetterData/DocInfo/DocName == 'NIGO Death Initial Notification Letter' || LetterData/DocInfo/DocName == 'Explanation of Benefit Letter Amount') ,TitleCase(LetterData/M_Name),LetterData/M_Name)"));
        node1.add(entry("PlanCode_Lookup/ServiceCenterHours", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 25, \"\")"));
        node1.add(entry("PlanCode_Lookup/Company_Text", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 11, \"\")"));
        node1.add(entry("PlanCode_Lookup/FaxNumber", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 26, \"\")"));
        node1.add(entry("LetterData/CurrentDate", "if(LetterData/CurrentDate == '' ||  LetterData/ClientId == 'SBUL' || LetterData/ClientId == 'FNWL' || LetterData/ClientId == 'WELB' || LetterData/ClientId == 'ELIC'|| LetterData/ClientId == 'SBL', Now('MMMM dd, yyyy'), MaskDateTime(LetterData/CurrentDate, 'yyyy-MM-dd', 'MMMM dd, yyyy'))"));
        node1.add(entry("LetterData/M_Recipient_FullName", "TitleCase(LetterData/M_Recipient_FullName)"));
        node1.add(entry("LetterData/M_Recipient_Zip", "if(Length(LetterData/M_Recipient_Zip) <= 5, LetterData/M_Recipient_Zip, Insert(LetterData/M_Recipient_Zip,6,'-'))"));
        node1.add(entry("PlanCode_Lookup/PhoneNumber", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 23, \"\")"));
        node1.add(entry("PlanCode_Lookup/Address", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 17, \"\")"));
        node1.add(entry("PlanCode_Lookup/ServiceCenter","Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 24, \"\")"));

        // === Node 2: Address transformations with multiple scripts on same variable ===
        List<Map.Entry<String, String>> node2 = new ArrayList<>();
        node2.add(entry("PlanCode_Lookup/Client_Abbr", "Lookup(LetterData/PlanCode, 'PlanCode_Lookup', 36, \"\")"));
        // Multiple scripts on PlanCode_Lookup/Address - each uses the result from the previous
        node2.add(entry("PlanCode_Lookup/Address", "Replace(PlanCode_Lookup/Address, 'ENDLINE', '\r\n')"));
        node2.add(entry("PlanCode_Lookup/Address", "Replace(PlanCode_Lookup/Address, 'comma', ',')"));
        node2.add(entry("PlanCode_Lookup/Address", "if(LetterData/ClientId == 'NASU' && IndexOf(PlanCode_Lookup/Address, 'Nassau Life and Annuity Company') != 0 && LetterData/IssueState == 'CA', Replace(PlanCode_Lookup/Address, 'Nassau Life and Annuity Company', 'Nassau Life and Annuity Insurance Company'), PlanCode_Lookup/Address)"));
        
        List<Map.Entry<String, String>> node3 = new ArrayList<>();
        node3.add(entry("LetterData/GR_Slife", ""));
        node3.add(entry("LetterData/GR3", ""));
        node3.add(entry("LetterData/BR43", ""));
        
        nodes.add(node1);
        nodes.add(node2);
        nodes.add(node3);

        return nodes;
    }

    /**
     * Helper method to create Map.Entry instances for node definitions
     */
    private static Map.Entry<String, String> entry(String path, String script) {
        return new AbstractMap.SimpleEntry<>(path, script);
    }

    // =========================================================
    // HTML entities
    // =========================================================
    static final class Html {
        static String decodeEntities(String s) {
            return s.replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&quot;", "\"")
                    .replace("&apos;", "'")
                    .replace("&amp;", "&");
        }
    }

    // =========================================================
    // JSON Utilities
    // =========================================================
    static final class JsonUtil {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        // ----- File I/O -----

        /**
         * Load input.json from resources/input/ folder
         */
        static ObjectNode loadInputJson() {
            // Try classpath first
            String[] classpathPaths = { "/" + INPUT_DIR + "/" + INPUT_FILE, "/input/" + INPUT_FILE };
            for (String p : classpathPaths) {
                try (InputStream in = ExprEval.class.getResourceAsStream(p)) {
                    if (in != null) {
                        JsonNode node = MAPPER.readTree(in);
                        if (node instanceof ObjectNode) return (ObjectNode) node;
                        throw new EvalException("PARSE_ERROR: input.json must be a JSON object");
                    }
                } catch (IOException ignored) {}
            }

            // Try filesystem
            String[] fsPaths = { INPUT_DIR + "/" + INPUT_FILE, "src/main/resources/" + INPUT_DIR + "/" + INPUT_FILE };
            for (String p : fsPaths) {
                File f = new File(p);
                if (f.isFile()) {
                    try {
                        JsonNode node = MAPPER.readTree(f);
                        if (node instanceof ObjectNode) return (ObjectNode) node;
                        throw new EvalException("PARSE_ERROR: input.json must be a JSON object");
                    } catch (IOException e) {
                        throw new EvalException("PARSE_ERROR: cannot read " + p + " - " + e.getMessage());
                    }
                }
            }

            throw new EvalException("PARSE_ERROR: cannot find input.json in resources/" + INPUT_DIR + "/");
        }

        /**
         * Save JSON to intermediate folder
         */
        static void saveIntermediateJson(ObjectNode json, String filename) {
            saveToDir(json, INTERMEDIATE_DIR, filename);
        }

        /**
         * Save final output.json
         */
        static void saveOutputJson(ObjectNode json) {
            saveToDir(json, OUTPUT_DIR, OUTPUT_FILE);
        }

        /**
         * Clear intermediate directory
         */
        static void clearIntermediateDir() {
            Path dir = resolveOutputDir(INTERMEDIATE_DIR);
            if (Files.exists(dir)) {
                try {
                    Files.walk(dir)
                         .filter(Files::isRegularFile)
                         .filter(p -> p.toString().endsWith(".json"))
                         .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
                } catch (IOException ignored) {}
            }
        }

        private static void saveToDir(ObjectNode json, String dirName, String filename) {
            Path dir = resolveOutputDir(dirName);
            try {
                Files.createDirectories(dir);
                Path file = dir.resolve(filename);
                MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), json);
            } catch (IOException e) {
                throw new EvalException("IO_ERROR: cannot write to " + dirName + "/" + filename + " - " + e.getMessage());
            }
        }

        private static Path resolveOutputDir(String dirName) {
            // Prefer src/main/resources for development, fall back to current dir
            Path srcResources = Paths.get("src/main/resources", dirName);
            if (Files.exists(srcResources.getParent())) {
                return srcResources;
            }
            return Paths.get(dirName);
        }

        // ----- Path Resolution -----

        /**
         * Resolves a slash-separated path against a JSON tree.
         * Example: "LetterData/M_Amount" navigates to root.LetterData.M_Amount
         * Returns empty string if path not found or value is null/empty.
         */
        static String resolvePath(JsonNode root, String path) {
            if (root == null || path == null || path.isEmpty()) return "";
            String[] parts = path.split("/");
            if (parts.length == 0) return "";

            JsonNode current = root;
            for (String part : parts) {
                if (part.isEmpty()) continue;
                if (current == null || current.isNull() || current.isMissingNode()) return "";

                if (current.isObject()) {
                    current = current.get(part);
                } else if (current.isArray()) {
                    // Array indexing support (for future use): path segment could be numeric index
                    try {
                        int index = Integer.parseInt(part);
                        current = current.get(index);
                    } catch (NumberFormatException e) {
                        return ""; // cannot navigate array with non-numeric key
                    }
                } else {
                    return ""; // cannot navigate further into a value node
                }
            }

            if (current == null || current.isNull() || current.isMissingNode()) return "";
            return getStringValue(current);
        }

        /**
         * Sets a value at the specified path, creating intermediate objects if they don't exist.
         * Example: setValueAtPath(root, "LetterData/NewField", "value")
         *          creates LetterData object if needed, then sets NewField = "value"
         */
        static void setValueAtPath(ObjectNode root, String path, String value) {
            if (root == null || path == null || path.isEmpty()) {
                throw new EvalException("INVALID_PATH: path cannot be null or empty");
            }

            String[] parts = path.split("/");
            if (parts.length == 0) {
                throw new EvalException("INVALID_PATH: path cannot be empty");
            }

            ObjectNode current = root;

            // Navigate/create all intermediate objects except the last part
            for (int i = 0; i < parts.length - 1; i++) {
                String part = parts[i];
                if (part.isEmpty()) continue;

                JsonNode child = current.get(part);
                if (child == null || child.isNull() || child.isMissingNode()) {
                    // Create new object node
                    ObjectNode newNode = MAPPER.createObjectNode();
                    current.set(part, newNode);
                    current = newNode;
                } else if (child.isObject()) {
                    current = (ObjectNode) child;
                } else {
                    throw new EvalException("PATH_ERROR: cannot navigate through non-object at '" + part + "' in path: " + path);
                }
            }

            // Set the value at the final path segment
            String lastPart = parts[parts.length - 1];
            current.put(lastPart, value);
        }

        private static String getStringValue(JsonNode node) {
            if (node == null || node.isNull() || node.isMissingNode()) return "";
            if (node.isTextual()) return node.asText();
            if (node.isNumber()) return node.asText();
            if (node.isBoolean()) return String.valueOf(node.asBoolean());
            if (node.isArray() || node.isObject()) {
                // Return JSON string representation for complex types
                return node.toString();
            }
            return "";
        }

        /**
         * Deep copy an ObjectNode
         */
        static ObjectNode deepCopy(ObjectNode node) {
            return node.deepCopy();
        }
    }

    // =========================================================
    // Values & Coercion
    // =========================================================
    enum Type { BOOLEAN, NUMERIC, STRING }
    interface Value { Type type(); }

    static final class BoolVal implements Value {
        final boolean v; BoolVal(boolean v){ this.v = v; }
        public Type type(){ return Type.BOOLEAN; }
        public String toString(){ return Boolean.toString(v); }
    }
    static final class NumVal implements Value {
        final BigDecimal v;
        NumVal(BigDecimal v){ this.v = v; }
        static NumVal ofInt(long x){ return new NumVal(BigDecimal.valueOf(x)); }
        public Type type(){ return Type.NUMERIC; }
        public String toString(){ return v.toPlainString(); }
    }
    static final class StrVal implements Value {
        final String v; StrVal(String v){ this.v = v == null ? "" : v; }
        public Type type(){ return Type.STRING; }
        public String toString(){ return v; }
    }

    static final class ValuePrinter {
        static String print(Value val) {
            if (val == null) return "";
            if (val instanceof StrVal) return ((StrVal) val).v;
            if (val instanceof BoolVal) return Boolean.toString(((BoolVal) val).v);
            if (val instanceof NumVal) return ((NumVal) val).v.stripTrailingZeros().toPlainString();
            return val.toString();
        }
    }

    static final class Coerce {
        static boolean isNumeric(Value v){
            if (v instanceof NumVal) return true;
            if (v instanceof StrVal) return parseBigDecimalOrNull(((StrVal) v).v) != null;
            return false;
        }
        static BigDecimal toBigDecimal(Value v){
            if (v instanceof NumVal) return ((NumVal) v).v;
            if (v instanceof StrVal) {
                BigDecimal bd = parseBigDecimalOrNull(((StrVal) v).v);
                if (bd == null) throw new EvalException("TYPE_ERROR: not numeric: " + ((StrVal) v).v);
                return bd;
            }
            if (v instanceof BoolVal) return ((BoolVal) v).v ? BigDecimal.ONE : BigDecimal.ZERO;
            throw new EvalException("TYPE_ERROR: cannot coerce to numeric");
        }
        static String toString(Value v){
            if (v instanceof StrVal) return ((StrVal) v).v;
            if (v instanceof NumVal) return ((NumVal) v).v.stripTrailingZeros().toPlainString();
            if (v instanceof BoolVal) return Boolean.toString(((BoolVal) v).v);
            return String.valueOf(v);
        }
        static boolean toBoolean(Value v){
            if (v instanceof BoolVal) return ((BoolVal) v).v;
            if (v instanceof NumVal) return ((NumVal) v).v.compareTo(BigDecimal.ZERO) != 0;
            if (v instanceof StrVal) {
                String s = ((StrVal) v).v.trim();
                if (s.equalsIgnoreCase("true")) return true;
                if (s.equalsIgnoreCase("false")) return false;
                BigDecimal bd = parseBigDecimalOrNull(s);
                if (bd != null) return bd.compareTo(BigDecimal.ZERO) != 0;
                return !s.isEmpty();
            }
            return false;
        }
        private static BigDecimal parseBigDecimalOrNull(String s){
            if (s == null) return null;
            String t = s.trim();
            if (t.isEmpty()) return BigDecimal.ZERO;
            try { return new BigDecimal(t); } catch (Exception e) { return null; }
        }
    }

    // =========================================================
    // Exceptions
    // =========================================================
    static final class EvalException extends RuntimeException {
        EvalException(String msg){ super(msg); }
    }

    // =========================================================
    // Lexer / Parser
    // =========================================================
    enum TokType {
        IDENT, NUMBER, STRING, TRUE, FALSE,
        LPAREN, RPAREN, COMMA,
        PLUS, MINUS, STAR, SLASH, PERCENT,
        LT, LE, GT, GE, EQ, NE,
        ANDAND, OROR,
        EOF
    }
    static final class Token {
        final TokType type; final String text;
        Token(TokType t, String s){ type = t; text = s; }
        public String toString(){ return type + (text != null ? "(" + text + ")" : ""); }
    }
    static final class Lexer {
        private final String s; private int i=0;
        Lexer(String s){ this.s = s; }
        Token next(){
            skipWs(); if (i >= s.length()) return new Token(TokType.EOF, null);
            char c = s.charAt(i);
            if (isIdentStart(c)){
                int start=i++; while(i<s.length() && isIdentPart(s.charAt(i))) i++;
                String ident = s.substring(start,i);
                if ("true".equals(ident)) return new Token(TokType.TRUE, ident);
                if ("false".equals(ident)) return new Token(TokType.FALSE, ident);
                return new Token(TokType.IDENT, ident);
            }
            if (Character.isDigit(c)){
                int start = i++;
                while (i<s.length() && Character.isDigit(s.charAt(i))) i++;
                if (i<s.length() && s.charAt(i)=='.'){
                    int j = i+1;
                    if (j<s.length() && Character.isDigit(s.charAt(j))){
                        i = j+1;
                        while (i<s.length() && Character.isDigit(s.charAt(i))) i++;
                    }
                }
                return new Token(TokType.NUMBER, s.substring(start,i));
            }
            if (c=='\'' || c=='"'){
                char q=c; i++; StringBuilder sb = new StringBuilder();
                while (i<s.length()){
                    char ch = s.charAt(i++);
                    if (ch==q) break;
                    if (ch=='\\'){
                        if (i>=s.length()) throw new EvalException("PARSE_ERROR: unfinished escape");
                        char e = s.charAt(i++);
                        switch(e){
                            case '\\': sb.append('\\'); break;
                            case '\'': sb.append('\''); break;
                            case '"': sb.append('"'); break;
                            case 'n': sb.append('\n'); break;
                            case 'r': sb.append('\r'); break;
                            case 't': sb.append('\t'); break;
                            case 'u': {
                                if (i+3>=s.length()) throw new EvalException("PARSE_ERROR: bad \\u escape");
                                String hex = s.substring(i, i+4); i+=4;
                                try { sb.append((char) Integer.parseInt(hex,16)); } catch(Exception ex){ throw new EvalException("PARSE_ERROR: bad \\u escape"); }
                                break;
                            }
                            default: sb.append(e);
                        }
                    } else sb.append(ch);
                }
                return new Token(TokType.STRING, sb.toString());
            }
            if (i+1 < s.length()){
                String two = s.substring(i, i+2);
                switch(two){
                    case "&&": i+=2; return new Token(TokType.ANDAND, "&&");
                    case "||": i+=2; return new Token(TokType.OROR, "||");
                    case "==": i+=2; return new Token(TokType.EQ, "==");
                    case "!=": i+=2; return new Token(TokType.NE, "!=");
                    case "<=": i+=2; return new Token(TokType.LE, "<=");
                    case ">=": i+=2; return new Token(TokType.GE, ">=");
                }
            }
            i++;
            switch(c){
                case '(' : return new Token(TokType.LPAREN,"(");
                case ')' : return new Token(TokType.RPAREN,")");
                case ',' : return new Token(TokType.COMMA,",");
                case '+' : return new Token(TokType.PLUS,"+");
                case '-' : return new Token(TokType.MINUS,"-");
                case '*' : return new Token(TokType.STAR,"*");
                case '/' : return new Token(TokType.SLASH,"/");
                case '%' : return new Token(TokType.PERCENT,"%");
                case '<' : return new Token(TokType.LT,"<");
                case '>' : return new Token(TokType.GT,">");
                case '&' :
                case '|' : throw new EvalException("PARSE_ERROR: single '&' or '|' not allowed");
                default: throw new EvalException("PARSE_ERROR: unexpected char '" + c + "'");
            }
        }
        private void skipWs(){ while(i<s.length()){ char c=s.charAt(i); if (c==' '||c=='\t'||c=='\n'||c=='\r') i++; else break; } }
        private static boolean isIdentStart(char c){ return Character.isLetter(c) || c=='_'; }
        private static boolean isIdentPart(char c){ return Character.isLetterOrDigit(c)||c=='_'; }
    }

    interface ExprNode { Value eval(Context ctx); }

    static final class Parser {
        private final Lexer lex; private Token la;
        Parser(Lexer lex){ this.lex = lex; la = lex.next(); }
        private Token eat(TokType t){ if (la.type != t) throw new EvalException("PARSE_ERROR: expected " + t + " but got " + la.type); Token cur=la; la = lex.next(); return cur; }
        private boolean look(TokType t){ return la.type == t; }
        ExprNode parse(){ ExprNode n = parseExpr(); if (la.type != TokType.EOF) throw new EvalException("PARSE_ERROR: unexpected token " + la); return n; }
        private ExprNode parseExpr(){ return parseOr(); }
        private ExprNode parseOr(){ ExprNode left = parseAnd(); while(look(TokType.OROR)){ eat(TokType.OROR); left = new BinOp("||", left, parseAnd()); } return left; }
        private ExprNode parseAnd(){ ExprNode left = parseRel(); while(look(TokType.ANDAND)){ eat(TokType.ANDAND); left = new BinOp("&&", left, parseRel()); } return left; }
        private ExprNode parseRel(){
            ExprNode left = parseAdd();
            while(true){
                if (look(TokType.EQ)){ eat(TokType.EQ); left = new BinOp("==", left, parseAdd()); }
                else if (look(TokType.NE)){ eat(TokType.NE); left = new BinOp("!=", left, parseAdd()); }
                else if (look(TokType.LT)){ eat(TokType.LT); left = new BinOp("<", left, parseAdd()); }
                else if (look(TokType.LE)){ eat(TokType.LE); left = new BinOp("<=", left, parseAdd()); }
                else if (look(TokType.GT)){ eat(TokType.GT); left = new BinOp(">", left, parseAdd()); }
                else if (look(TokType.GE)){ eat(TokType.GE); left = new BinOp(">=", left, parseAdd()); }
                else break;
            }
            return left;
        }
        private ExprNode parseAdd(){
            ExprNode left = parseUnary();
            while(true){
                if (look(TokType.PLUS)){ eat(TokType.PLUS); left = new BinOp("+", left, parseUnary()); }
                else if (look(TokType.MINUS)){ eat(TokType.MINUS); left = new BinOp("-", left, parseUnary()); }
                else break;
            }
            return left;
        }
        private ExprNode parseUnary(){ if (look(TokType.MINUS)){ eat(TokType.MINUS); return new UnaryOp("-", parseUnary()); } return parsePrimary(); }
        private ExprNode parsePrimary(){
            switch(la.type){
                case TRUE: eat(TokType.TRUE); return new BoolLit(true);
                case FALSE: eat(TokType.FALSE); return new BoolLit(false);
                case NUMBER: return new NumLit(new BigDecimal(eat(TokType.NUMBER).text));
                case STRING: return new StrLit(eat(TokType.STRING).text);
                case IDENT: {
                    String ident = eat(TokType.IDENT).text;
                    if (look(TokType.LPAREN)){
                        eat(TokType.LPAREN);
                        List<ExprNode> args = new ArrayList<>();
                        if (!look(TokType.RPAREN)){
                            args.add(parseExpr());
                            while(look(TokType.COMMA)){ eat(TokType.COMMA); args.add(parseExpr()); }
                        }
                        eat(TokType.RPAREN);
                        return new FuncCall(ident, args);
                    } else {
                        // Parse slash-separated path for JSON field access (e.g., "LetterData/M_Amount")
                        List<String> segs = new ArrayList<>();
                        segs.add(ident);
                        while (look(TokType.SLASH)){
                            eat(TokType.SLASH);
                            segs.add(eat(TokType.IDENT).text);
                        }
                        return new VarRef(String.join("/", segs));
                    }
                }
                case LPAREN: { eat(TokType.LPAREN); ExprNode n = parseExpr(); eat(TokType.RPAREN); return n; }
                default: throw new EvalException("PARSE_ERROR: unexpected token " + la);
            }
        }
    }

    // =========================================================
    // AST nodes
    // =========================================================
    static final class BoolLit implements ExprNode { final boolean v; BoolLit(boolean v){this.v=v;} public Value eval(Context ctx){ return new BoolVal(v); } }
    static final class NumLit implements ExprNode { final BigDecimal v; NumLit(BigDecimal v){this.v=v;} public Value eval(Context ctx){ return new NumVal(v); } }
    static final class StrLit implements ExprNode { final String v; StrLit(String v){this.v=v;} public Value eval(Context ctx){ return new StrVal(v); } }
    static final class VarRef implements ExprNode {
        final String path;
        VarRef(String path) { this.path = path; }
        
        public Value eval(Context ctx) {
            String val;
            // If this path matches the current target path (self-reference), read from intermediate JSON
            // Otherwise (cross-reference), read from output JSON (previous node's snapshot)
            if (path.equals(ctx.currentTargetPath)) {
                val = JsonUtil.resolvePath(ctx.intermediateJson, path);
            } else {
                val = JsonUtil.resolvePath(ctx.outputJson, path);
            }
            return new StrVal(val == null ? "" : val);
        }
    }
    static final class UnaryOp implements ExprNode {
        final String op; final ExprNode e;
        UnaryOp(String op, ExprNode e){ this.op=op; this.e=e; }
        public Value eval(Context ctx){
            Value v = e.eval(ctx);
            if ("-".equals(op)) {
                BigDecimal bd = Coerce.toBigDecimal(v);
                return new NumVal(bd.negate());
            }
            throw new EvalException("PARSE_ERROR: unknown unary op " + op);
        }
    }
    static final class BinOp implements ExprNode {
        final String op; final ExprNode l, r;
        BinOp(String op, ExprNode l, ExprNode r){ this.op=op; this.l=l; this.r=r; }
        public Value eval(Context ctx){
            Value lv = l.eval(ctx), rv = r.eval(ctx);
            switch(op){
                case "+": {
                    boolean ln = Coerce.isNumeric(lv), rn = Coerce.isNumeric(rv);
                    if (ln && rn) {
                        return new NumVal(Coerce.toBigDecimal(lv).add(Coerce.toBigDecimal(rv)));
                    }
                    return new StrVal(Coerce.toString(lv) + Coerce.toString(rv));
                }
                case "-": {
                    BigDecimal a = Coerce.toBigDecimal(lv), b = Coerce.toBigDecimal(rv);
                    return new NumVal(a.subtract(b));
                }
                case "*": {
                    BigDecimal a = Coerce.toBigDecimal(lv), b = Coerce.toBigDecimal(rv);
                    return new NumVal(a.multiply(b));
                }
                case "/": {
                    BigDecimal a = Coerce.toBigDecimal(lv), b = Coerce.toBigDecimal(rv);
                    if (b.compareTo(BigDecimal.ZERO)==0) throw new EvalException("DIVIDE_BY_ZERO");
                    return new NumVal(a.divide(b, 10, RoundingMode.HALF_UP));
                }
                case "%": {
                    BigDecimal a = Coerce.toBigDecimal(lv), b = Coerce.toBigDecimal(rv);
                    try {
                        BigInteger ai = a.toBigIntegerExact(), bi = b.toBigIntegerExact();
                        if (bi.equals(BigInteger.ZERO)) throw new EvalException("DIVIDE_BY_ZERO");
                        return new NumVal(new BigDecimal(ai.mod(bi)));
                    } catch (ArithmeticException ex) {
                        throw new EvalException("TYPE_ERROR: '%' expects integer operands");
                    }
                }
                case "==":
                case "!=": {
                    boolean eq;
                    if (Coerce.isNumeric(lv) && Coerce.isNumeric(rv)) {
                        eq = Coerce.toBigDecimal(lv).compareTo(Coerce.toBigDecimal(rv)) == 0;
                    } else if (lv instanceof BoolVal || rv instanceof BoolVal) {
                        eq = Coerce.toBoolean(lv) == Coerce.toBoolean(rv);
                    } else {
                        eq = Coerce.toString(lv).equals(Coerce.toString(rv));
                    }
                    return new BoolVal("==".equals(op) ? eq : !eq);
                }
                case "<":
                case "<=":
                case ">":
                case ">=": {
                    int c;
                    if (Coerce.isNumeric(lv) && Coerce.isNumeric(rv)) {
                        c = Coerce.toBigDecimal(lv).compareTo(Coerce.toBigDecimal(rv));
                    } else if (lv instanceof StrVal && rv instanceof StrVal) {
                        c = ((StrVal) lv).v.compareTo(((StrVal) rv).v);
                    } else {
                        throw new EvalException("TYPE_ERROR: ordering requires both numeric or both string");
                    }
                    boolean res;
                    switch(op){
                        case "<": res = c < 0; break;
                        case "<=": res = c <= 0; break;
                        case ">": res = c > 0; break;
                        case ">=": res = c >= 0; break;
                        default: throw new EvalException("PARSE_ERROR: bad op");
                    }
                    return new BoolVal(res);
                }
                case "&&":
                case "||": {
                    boolean a = Coerce.toBoolean(lv), b = Coerce.toBoolean(rv);
                    return new BoolVal("&&".equals(op) ? (a && b) : (a || b));
                }
                default: throw new EvalException("PARSE_ERROR: unknown operator " + op);
            }
        }
    }

    // =========================================================
    // Functions
    // =========================================================
    interface Function { Value invoke(Context ctx, List<Value> args); }
    static final class FunctionRegistry {
        private final Map<String, Function> map = new HashMap<>();
        void register(String name, Function f){ map.put(name.toLowerCase(Locale.ROOT), f); }
        Function resolve(String name){
            Function f = map.get(name.toLowerCase(Locale.ROOT));
            if (f == null) throw new EvalException("UNKNOWN_FUNCTION: '" + name + "'");
            return f;
        }
    }
    static final class FuncCall implements ExprNode {
        final String name; final List<ExprNode> args;
        FuncCall(String n, List<ExprNode> a){ name=n; args=a; }
        public Value eval(Context ctx){
            // Lazy evaluation for If function - only evaluate the selected branch
            if ("if".equals(name.toLowerCase())) {
                if (args.size() != 3) throw new EvalException("ARITY_MISMATCH: 'If' expects 3 args");
                Value condVal = args.get(0).eval(ctx);
                boolean cond = Coerce.toBoolean(condVal);
                // Only evaluate the branch that will be used
                return cond ? args.get(1).eval(ctx) : args.get(2).eval(ctx);
            }
            // For all other functions, evaluate all arguments
            Function f = ctx.functions.resolve(name);
            List<Value> av = new ArrayList<>(args.size());
            for (ExprNode n : args) av.add(n.eval(ctx));
            
            try {
                return f.invoke(ctx, av);
            } catch (EvalException e) {
                // Check if any argument is an empty string
                boolean hasEmptyInput = av.stream().anyMatch(v -> 
                    v instanceof StrVal && ((StrVal) v).v.trim().isEmpty());
                if (hasEmptyInput) {
                    // Log warning and return empty string instead of breaking execution
                    System.err.println("WARNING: " + name + "() - " + e.getMessage() + " (empty input, returning empty string)");
                    return new StrVal("");
                }
                // Re-throw if no empty input was involved
                throw e;
            }
        }
    }
    static final class Context {
        final JsonNode outputJson;           // Snapshot from previous node (for cross-variable references)
        final ObjectNode intermediateJson;   // Evolving state within current node (for self-references)
        final String currentTargetPath;      // The variable being modified by current script
        final FunctionRegistry functions;
        
        Context(JsonNode outputJson, ObjectNode intermediateJson, String currentTargetPath, FunctionRegistry functions) {
            this.outputJson = outputJson;
            this.intermediateJson = intermediateJson;
            this.currentTargetPath = currentTargetPath;
            this.functions = functions;
        }
    }

    static final class StandardFunctions {

        // ---------- Supported masks for autodetect ----------
        static final List<String> SUPPORTED_INPUT_MASKS_ORDER = Arrays.asList(
                "yyyy-MM-dd'T'HH:mm:ss",
                "MM-dd-yyyy",
                "MMMM dd, yyyy",
                "MM/dd/yyyy",
                "yyyy-MM-dd",
                "HH:mm:ss",
                "hh:mm:ss a"
        );
        static final Set<String> OUTPUT_ONLY_MASKS = new HashSet<>(Arrays.asList("yyyy"));

        static void registerAll(FunctionRegistry r){

            // ----------------- Control -----------------
            r.register("If", (ctx,a)->{
                ensureArityBetween("If", a, 3, 3);
                boolean cond = Coerce.toBoolean(a.get(0));
                return cond ? a.get(1) : a.get(2);
            });

            // ----------------- Strings -----------------
            r.register("Concat", (ctx,a)->{
                if (a.isEmpty()) throw new EvalException("ARITY_MISMATCH: 'Concat' expects >=1 args");
                StringBuilder sb = new StringBuilder();
                for (Value v : a) sb.append(Coerce.toString(v));
                return new StrVal(sb.toString());
            });
            r.register("UpperCase", (ctx,a)->{ ensureArity("UpperCase",a,1); return new StrVal(Coerce.toString(a.get(0)).toUpperCase(Locale.ROOT)); });
            r.register("LowerCase", (ctx,a)->{ ensureArity("LowerCase",a,1); return new StrVal(Coerce.toString(a.get(0)).toLowerCase(Locale.ROOT)); });
            r.register("TitleCase", (ctx,a)->{ ensureArity("TitleCase",a,1); return new StrVal(toTitleCase(Coerce.toString(a.get(0)))); });
            r.register("Replace", (ctx,a)->{
                if (a.size()==3) {
                    // Existing syntax: Replace(Input, SearchStr, ReplaceStr)
                    String input  = Coerce.toString(a.get(0));
                    String search = Coerce.toString(a.get(1));
                    String repl   = Coerce.toString(a.get(2));
                    return new StrVal(input.replace(search, repl));
                } else if (a.size()==4) {
                    // New syntax: Replace(Input, Index(1-based), Length, ReplaceStr)
                    String input = Coerce.toString(a.get(0));
                    int index1   = toIntSafe(Coerce.toBigDecimal(a.get(1))); // 1-based index
                    int len      = toIntSafe(Coerce.toBigDecimal(a.get(2)));
                    String repl  = Coerce.toString(a.get(3));

//                    if (index1 < 1) throw new EvalException("RANGE_ERROR: Replace index out of range");
                    if (index1 < 1) index1 = 1;
//                    if (len < 0)   throw new EvalException("RANGE_ERROR: Replace length negative");
                    if (len < 0)   len = 0;

                    int start0 = index1 - 1;                   // convert to 0-based
//                    if (start0 > input.length()) throw new EvalException("RANGE_ERROR: Replace start out of range");
                    if (start0 > input.length()) start0 = input.length();

                    int end0 = start0 + len;
//                    if (end0 > input.length()) throw new EvalException("RANGE_ERROR: Replace end out of range");
                    if (end0 > input.length()) end0 = input.length();

                    return new StrVal(input.substring(0, start0) + repl + input.substring(end0));
                } else {
                    throw new EvalException("ARITY_MISMATCH: 'Replace' expects 3 or 4 args");
                }
            });

            // Trim(Input[, TrimChar])  (default TrimChar = space)
            r.register("Trim", (ctx,a)->{
                if (a.size()!=1 && a.size()!=2) throw new EvalException("ARITY_MISMATCH: 'Trim' expects 1 or 2 args");
                String s = Coerce.toString(a.get(0));
                if (a.size()==1) return new StrVal(s.trim());
                String chars = Coerce.toString(a.get(1));
                if (chars.isEmpty()) return new StrVal(s);
                char ch = chars.charAt(0); // spec says "character"
                return new StrVal(trimChar(s, ch));
            });

            // Length(Input) -> Numeric (usable in string contexts)
            r.register("Length", (ctx,a)->{ ensureArity("Length",a,1); return new NumVal(new BigDecimal(Coerce.toString(a.get(0)).length())); });

            // Insert(Input, Index(1-based), InsertStr)
            r.register("Insert", (ctx,a)->{
                ensureArity("Insert",a,3);
                String input = Coerce.toString(a.get(0));
                String ins = Coerce.toString(a.get(2));
                int idx1 = toIntExact(Coerce.toBigDecimal(a.get(1))); // 1-based index per spec (Index, InsertStr order from sheet is (Input,Index,InsertStr))
                int at = idx1 - 1;
//                if (at < 0 || at > input.length()) throw new EvalException("RANGE_ERROR: Insert index out of range");
                if (at < 0) return new StrVal(ins+input);
                if (at > input.length()) return new StrVal(input+ins);
                return new StrVal(input.substring(0, at) + ins + input.substring(at));
            });

            // IndexOf(Input,SearchStr[,StartIndex(1-based)]) -> location 1-based; -1 if not found
            r.register("IndexOf", (ctx,a)->{
                if (a.size()!=2 && a.size()!=3) throw new EvalException("ARITY_MISMATCH: 'IndexOf' expects 2 or 3 args");
                String s = Coerce.toString(a.get(0)), t = Coerce.toString(a.get(1));
                int from0 = 0;
                if (a.size()==3) {
                    int start1 = toIntSafe(Coerce.toBigDecimal(a.get(2))); // 1-based "after that index"
                    if (start1 < 0) start1 = 0; // default "0" means before string start
                    from0 = start1; // start after that index => 0-based from = start1
                    if (from0 > s.length()) return new NumVal(BigDecimal.valueOf(-1));
                }
                int pos0 = s.indexOf(t, from0);
                return new NumVal(BigDecimal.valueOf(pos0 < 0 ? -1 : (pos0 + 1)));
            });

            // LastIndexOf(Input,SearchStr[,StartIndex(1-based)]) -> location 1-based; -1 if not found
            r.register("LastIndexOf", (ctx,a)->{
                if (a.size()!=2 && a.size()!=3) throw new EvalException("ARITY_MISMATCH: 'LastIndexOf' expects 2 or 3 args");
                String s = Coerce.toString(a.get(0)), t = Coerce.toString(a.get(1));
                int from0 = s.length() - 1;
                if (a.size()==3) {
                    int start1 = toIntSafe(Coerce.toBigDecimal(a.get(2)));
                    if (start1 < 1) from0 = -1;
                    else from0 = Math.min(s.length() - 1, start1 - 1); // Java lastIndexOf uses inclusive fromIndex (0-based)
                }
                int pos0 = s.lastIndexOf(t, from0);
                return new NumVal(BigDecimal.valueOf(pos0 < 0 ? -1 : (pos0 + 1)));
            });

            // SubString(Input, StartIndex(1-based)[, Length])
            r.register("SubString", (ctx,a)->{
                if (a.size()!=2 && a.size()!=3) throw new EvalException("ARITY_MISMATCH: 'SubString' expects 2 or 3 args");
                String s = Coerce.toString(a.get(0));
                int start1 = toIntSafe(Coerce.toBigDecimal(a.get(1)));
//                if (start1 < 1 || start1 > s.length()+0) throw new EvalException("RANGE_ERROR: SubString start out of range");
                if (start1 < 1 || start1 > s.length()+0) return new StrVal("");
                int start0 = start1 - 1;
                if (a.size()==2) return new StrVal(s.substring(start0));
                int len = toIntSafe(Coerce.toBigDecimal(a.get(2)));
//                if (len < 0) throw new EvalException("RANGE_ERROR: SubString length negative");
                if (len < 0) return new StrVal("");
                int end0 = start0 + len;
//                if (end0 > s.length()) throw new EvalException("RANGE_ERROR: SubString end out of range");
                if (end0 > s.length()) return new StrVal(s.substring(start0));
                return new StrVal(s.substring(start0, end0));
            });

            r.register("SubStringBefore", (ctx,a)->{ ensureArity("SubStringBefore",a,2);
                String s = Coerce.toString(a.get(0)), t = Coerce.toString(a.get(1));
                int i = s.indexOf(t); return new StrVal(i < 0 ? s : s.substring(0, i)); });

            r.register("SubStringAfter", (ctx,a)->{ ensureArity("SubStringAfter",a,2);
                String s = Coerce.toString(a.get(0)), t = Coerce.toString(a.get(1));
                int i = s.indexOf(t); return new StrVal(i < 0 ? s : s.substring(i + t.length())); });

            // ----------------- Numeric / Math (varargs) -----------------
            r.register("Add", (ctx,a)->{
                if (a.size() < 2) throw new EvalException("ARITY_MISMATCH: 'Add' expects >= 2 args");
                BigDecimal acc = BigDecimal.ZERO;
                for (Value v : a) acc = acc.add(Coerce.toBigDecimal(v));
                return new NumVal(acc);
            });

            r.register("Multiply", (ctx,a)->{
                if (a.size() < 2) throw new EvalException("ARITY_MISMATCH: 'Multiply' expects >= 2 args");
                BigDecimal acc = BigDecimal.ONE;
                for (Value v : a) acc = acc.multiply(Coerce.toBigDecimal(v));
                return new NumVal(acc);
            });

            r.register("Subtract", (ctx,a)->{
                if (a.size() < 2) throw new EvalException("ARITY_MISMATCH: 'Subtract' expects >= 2 args");
                BigDecimal acc = Coerce.toBigDecimal(a.get(0));
                for (int i=1;i<a.size();i++) acc = acc.subtract(Coerce.toBigDecimal(a.get(i)));
                return new NumVal(acc);
            });

            r.register("Divide", (ctx,a)->{
                if (a.size() < 2) throw new EvalException("ARITY_MISMATCH: 'Divide' expects >= 2 args");
                BigDecimal acc = Coerce.toBigDecimal(a.get(0));
                for (int i=1;i<a.size();i++) {
                    BigDecimal d = Coerce.toBigDecimal(a.get(i));
                    if (d.compareTo(BigDecimal.ZERO)==0) throw new EvalException("DIVIDE_BY_ZERO");
                    acc = acc.divide(d, 10, RoundingMode.HALF_UP);
                }
                return new NumVal(acc);
            });

            r.register("Mod", (ctx,a)->{
                if (a.size() < 2) throw new EvalException("ARITY_MISMATCH: 'Mod' expects >= 2 args");
                BigInteger acc;
                try { acc = Coerce.toBigDecimal(a.get(0)).toBigIntegerExact(); }
                catch (ArithmeticException e){ throw new EvalException("TYPE_ERROR: Mod expects integer operands"); }
                for (int i=1;i<a.size();i++){
                    BigInteger d;
                    try { d = Coerce.toBigDecimal(a.get(i)).toBigIntegerExact(); }
                    catch (ArithmeticException e){ throw new EvalException("TYPE_ERROR: Mod expects integer operands"); }
                    if (d.equals(BigInteger.ZERO)) throw new EvalException("DIVIDE_BY_ZERO");
                    acc = acc.mod(d);
                }
                return new NumVal(new BigDecimal(acc));
            });

            r.register("Abs", (ctx,a)->{ ensureArity("Abs",a,1); return new NumVal(Coerce.toBigDecimal(a.get(0)).abs()); });
            r.register("Floor", (ctx,a)->{ ensureArity("Floor",a,1); return new NumVal(Coerce.toBigDecimal(a.get(0)).setScale(0, RoundingMode.FLOOR)); });
            r.register("Ceil", (ctx,a)->{ ensureArity("Ceil",a,1); return new NumVal(Coerce.toBigDecimal(a.get(0)).setScale(0, RoundingMode.CEILING)); });

            // Round(Input[, Precision]); if Precision not provided -> return number as-is
            r.register("Round", (ctx,a)->{
                if (a.size()!=1 && a.size()!=2) throw new EvalException("ARITY_MISMATCH: 'Round' expects 1 or 2 args");
                BigDecimal x = Coerce.toBigDecimal(a.get(0));
                if (a.size()==1) return new NumVal(x); // per spec: no change if Precision unspecified
                int scale = toIntSafe(Coerce.toBigDecimal(a.get(1)));
                if (scale < 0) throw new EvalException("RANGE_ERROR: Round scale negative");
                return new NumVal(x.setScale(scale, RoundingMode.HALF_UP));
            });

            // ----------------- MaskNumber(Input, InputMask, OutputMask[, Language, Country]) -----------------
            r.register("MaskNumber", (ctx,a)->{
                if (a.size()<3 || a.size()>5) throw new EvalException("ARITY_MISMATCH: 'MaskNumber' expects 3..5 args");
                BigDecimal input = Coerce.toBigDecimal(a.get(0));
                String inputMask = Coerce.toString(a.get(1)); // currently unused; spec allows empty for plain numbers
                String mask = Coerce.toString(a.get(2));
                Locale loc = Locale.US; // Language/Country optional; default US
                if (a.size()>=4) { /*language*/ }
                if (a.size()>=5) { /*country*/ }
                return new StrVal(maskNumber(input, mask, loc));
            });

            // ----------------- MaskDateTime(Input, InputMask, OutputMask[, Language, Country]) -----------------
            r.register("MaskDateTime", (ctx,a)->{
                if (a.size()<3 || a.size()>5) throw new EvalException("ARITY_MISMATCH: 'MaskDateTime' expects 3..5 args");
                String input = Coerce.toString(a.get(0));
                String inMask = Coerce.toString(a.get(1));
                String outMask = Coerce.toString(a.get(2));
                // Language/Country optional; enforced English month names by Locale.ENGLISH in formatter
                return new StrVal(maskDateTime(input, inMask, outMask));
            });

            // ----------------- MaskPhoneNumber(Input, OutputMask) -----------------
            r.register("MaskPhoneNumber", (ctx,a)->{
                ensureArity("MaskPhoneNumber",a,2);
                String input = Coerce.toString(a.get(0));
                String mask = Coerce.toString(a.get(1)); // e.g., "(###) ###-####"
                return new StrVal(maskPhone(input, mask));
            });

            // ----------------- NumberToWords(Input[, Language, Country]) -----------------
            r.register("NumberToWords", (ctx,a)->{
                if (a.size()<1 || a.size()>3) throw new EvalException("ARITY_MISMATCH: 'NumberToWords' expects 1..3 args");
                BigInteger n;
                try { n = Coerce.toBigDecimal(a.get(0)).toBigIntegerExact(); }
                catch (ArithmeticException ex){ throw new EvalException("TYPE_ERROR: NumberToWords expects integer"); }
                // Language/Country optional; default English/US
                return new StrVal(englishWords(n));
            });

            // ----------------- Date/Time helpers -----------------

            // Now(Format)
            r.register("Now", (ctx,a)->{
                ensureArity("Now", a, 1);
                String fmt = Coerce.toString(a.get(0));
                DateTimeFormatter f = buildFormatter(fmt); // accept tokens incl. 'S'
                LocalDateTime now = LocalDateTime.now();
                return new StrVal(now.format(f));
            });

            // ToDateTime(SrcString, Format) => autodetect input; output in Format
            r.register("ToDateTime", (ctx,a)->{
                ensureArity("ToDateTime",a,2);
                String src = Coerce.toString(a.get(0));
                String outFmt = Coerce.toString(a.get(1));
                LocalDateTime dt = autodetectDateTime(src);
                DateTimeFormatter f = buildFormatter(outFmt);
                return new StrVal(dt.format(f));
            });

            // AddInterval(Input, Interval, DateField['year'|'month'|'day']) -> 'dd-MM-yyyy hh:mm:ss.SSS'
            r.register("AddInterval", (ctx,a)->{
                ensureArity("AddInterval",a,3);
                LocalDateTime dt = autodetectDateTime(Coerce.toString(a.get(0)));
                int amount = toIntSafe(Coerce.toBigDecimal(a.get(1)));
                String unit = Coerce.toString(a.get(2)).toLowerCase(Locale.ROOT);
                LocalDateTime res = addInterval(dt, amount, unit);
                DateTimeFormatter out = buildFormatter("dd-MM-yyyy hh:mm:ss.SSS");
                return new StrVal(res.format(out));
            });

            // SubtractInterval(Input, Interval, DateField['year'|'month'|'day']) -> 'dd-MM-yyyy hh:mm:ss.SSS'
            r.register("SubtractInterval", (ctx,a)->{
                ensureArity("SubtractInterval",a,3);
                LocalDateTime dt = autodetectDateTime(Coerce.toString(a.get(0)));
                int amount = toIntSafe(Coerce.toBigDecimal(a.get(1)));
                String unit = Coerce.toString(a.get(2)).toLowerCase(Locale.ROOT);
                LocalDateTime res = addInterval(dt, -amount, unit);
                DateTimeFormatter out = buildFormatter("dd-MM-yyyy hh:mm:ss.SSS");
                return new StrVal(res.format(out));
            });

            // Duration(Input1, Input2, DateField['year'|'month'|'day']) - both inputs must share the SAME input mask
            r.register("Duration", (ctx,a)->{
                ensureArity("Duration",a,3);
                String s1 = Coerce.toString(a.get(0));
                String s2 = Coerce.toString(a.get(1));
                String unit = Coerce.toString(a.get(2)).toLowerCase(Locale.ROOT);

                // find a single mask that parses BOTH inputs
                String mask = detectCommonDateMask(s1, s2);
                if (mask == null) throw new EvalException("DOMAIN_ERROR: Duration requires both inputs in the same supported mask");

                if (isTimeOnlyMask(mask))
                    throw new EvalException("DOMAIN_ERROR: Duration units year/month/day require date masks");

                LocalDateTime d1 = parseToDateTimeWith(mask, s1);
                LocalDateTime d2 = parseToDateTimeWith(mask, s2);

                BigDecimal out;
                switch(unit){
                    case "year":
                    case "years":
                        out = BigDecimal.valueOf(Period.between(d1.toLocalDate(), d2.toLocalDate()).getYears());
                        break;
                    case "month":
                    case "months":
                        Period p = Period.between(d1.toLocalDate(), d2.toLocalDate());
                        out = BigDecimal.valueOf(p.getYears()*12L + p.getMonths());
                        break;
                    case "day":
                    case "days":
                        out = BigDecimal.valueOf(ChronoUnit.DAYS.between(d1.toLocalDate(), d2.toLocalDate()));
                        break;
                    default:
                        throw new EvalException("DOMAIN_ERROR: DateField must be year|month|day");
                }
                return new NumVal(out);
            });

            // ----------------- Lookup(InputKey, LookupTableName, ColumnIndex[, DefaultValue]) -----------------
            r.register("Lookup", (ctx,a)->{
                if (a.size()<3 || a.size()>4) throw new EvalException("ARITY_MISMATCH: 'Lookup' expects 3 or 4 args");
                String inputKey = Coerce.toString(a.get(0));
                String tableName = Coerce.toString(a.get(1));
                int colIndex = toIntSafe(Coerce.toBigDecimal(a.get(2))); // 1-based
                String defaultVal = a.size()==4 ? Coerce.toString(a.get(3)) : "";

                if (colIndex < 1) return new StrVal(defaultVal);

                LookupTable table = LookupTableCache.get(tableName);
                if (table == null) {
                    table = LookupTable.load(tableName);
                    LookupTableCache.put(tableName, table);
                }

                String key = inputKey == null ? "" : inputKey.trim();
                String[] row = table.findRow(key);
                if (row == null) return new StrVal(defaultVal);
                if (colIndex > row.length) return new StrVal(defaultVal);
                String cell = row[colIndex - 1];
                return new StrVal(cell == null ? "" : cell);
            });
        }

        // ---------- helpers ----------
        static void ensureArity(String name, List<Value> a, int n){
            if (a.size()!=n) throw new EvalException("ARITY_MISMATCH: '" + name + "' expects " + n + " args");
        }
        static void ensureArityBetween(String name, List<Value> a, int lo, int hi){
            if (a.size()<lo || a.size()>hi) throw new EvalException("ARITY_MISMATCH: '" + name + "' expects " + lo + ".." + hi + " args");
        }
        static int toIntSafe(BigDecimal bd){ try { return bd.intValueExact(); } catch (ArithmeticException e){ return bd.setScale(0, RoundingMode.DOWN).intValue(); } }
        static int toIntExact(BigDecimal bd){ try { return bd.intValueExact(); } catch (ArithmeticException e){ throw new EvalException("TYPE_ERROR: expects integer"); } }

        static String toTitleCase(String s){
            if (s==null || s.isEmpty()) return s;
            StringBuilder out = new StringBuilder(s.length());
            boolean newWord = true;
            for (int i=0;i<s.length();i++){
                char c = s.charAt(i);
                if (Character.isLetterOrDigit(c)){
                    out.append(newWord ? Character.toTitleCase(c) : Character.toLowerCase(c));
                    newWord = false;
                } else { out.append(c); newWord = true; }
            }
            return out.toString();
        }
        static String trimChar(String s, char ch){
            int start=0, end=s.length();
            while (start<end && s.charAt(start)==ch) start++;
            while (end>start && s.charAt(end-1)==ch) end--;
            return s.substring(start, end);
        }

        // ----------------- MaskNumber -----------------
        static String maskNumber(BigDecimal input, String mask, Locale loc){
            if (mask == null || mask.isEmpty()) throw new EvalException("DOMAIN_ERROR: empty number mask");

            // Special all-zeros pattern: width & round/truncate examples
            if (mask.matches("^0+$")){
                int width = mask.length();
                String digits = roundToWidthAbs(input, width);
                return leftPadWithZeros(digits, width);
            }

            // Strict sign mirroring per spec
            boolean allowMinus = mask.indexOf('-') >= 0;
            BigDecimal val = input;
            if (!allowMinus && input.signum() < 0) val = input.abs();

            String dfPattern = mask.replace("-", "");
            DecimalFormatSymbols sym = DecimalFormatSymbols.getInstance(loc);
            DecimalFormat df = new DecimalFormat(dfPattern, sym);
            df.setRoundingMode(RoundingMode.HALF_UP);

            String out = df.format(val);
            if (allowMinus && input.signum() < 0) out = "-" + out;
            return out;
        }
        static String roundToWidthAbs(BigDecimal input, int width){
            BigDecimal abs = input.abs();
            String intPart = abs.setScale(0, RoundingMode.DOWN).toPlainString();
            int intDigits = intPart.equals("0") ? 1 : intPart.length();
            int p = intDigits - width;
            if (p >= 0){
                BigDecimal div = BigDecimal.TEN.pow(p);
                BigDecimal scaled = abs.divide(div, 0, RoundingMode.HALF_UP);
                return scaled.toPlainString();
            } else {
                BigDecimal rounded = abs.setScale(0, RoundingMode.HALF_UP);
                return rounded.toPlainString();
            }
        }
        static String leftPadWithZeros(String s, int width){
            String t = s.startsWith("-") ? s.substring(1) : s;
            StringBuilder sb = new StringBuilder();
            for (int i = t.length(); i < width; i++) sb.append('0');
            sb.append(t);
            return sb.toString();
        }

        // ----------------- MaskDateTime & Time tools -----------------
        static boolean isTimeOnlyMask(String m){
            return "HH:mm:ss".equals(m) || "hh:mm:ss a".equals(m);
        }
        static DateTimeFormatter buildFormatter(String mask){
            try {
                return DateTimeFormatter.ofPattern(mask, Locale.ENGLISH);
            } catch (IllegalArgumentException e){
                throw new EvalException("DOMAIN_ERROR: invalid date/time mask");
            }
        }
        static LocalDateTime autodetectDateTime(String input){
            for (String m : SUPPORTED_INPUT_MASKS_ORDER){
                try { return parseToDateTimeWith(m, input); } catch (Exception ignored){}
            }
            throw new EvalException("DOMAIN_ERROR: cannot detect input date/time mask");
        }
        static String maskDateTime(String input, String inputMask, String outputMask){
            // If input is empty or null, return empty string without validating masks
            if (input == null || input.trim().isEmpty()) {
                return "";
            }
            
            if (outputMask == null || outputMask.isEmpty()) throw new EvalException("DOMAIN_ERROR: empty date/time output mask");
            if ("yyyy".equals(inputMask)) throw new EvalException("DOMAIN_ERROR: 'yyyy' cannot be used as input mask");

            LocalDateTime dt;
            if (inputMask == null || inputMask.isEmpty()) {
                dt = autodetectDateTime(input);
            } else {
                dt = parseToDateTimeWith(inputMask, input);
            }
            DateTimeFormatter outFmt = buildFormatter(outputMask);
            return dt.format(outFmt);
        }
        static LocalDateTime parseToDateTimeWith(String fmt, String src){
            DateTimeFormatter f = buildFormatter(fmt);
            try {
                if (fmt.equals("HH:mm:ss") || fmt.equals("hh:mm:ss a")) {
                    LocalTime t = LocalTime.parse(src, f);
                    return LocalDateTime.of(LocalDate.of(1970,1,1), t);
                } else if (fmt.equals("yyyy")) {
                    throw new EvalException("DOMAIN_ERROR: 'yyyy' is not an input mask");
                } else {
                    try {
                        return LocalDateTime.parse(src, f);
                    } catch (DateTimeParseException e1){
                        LocalDate d = LocalDate.parse(src, f);
                        return d.atStartOfDay();
                    }
                }
            } catch (Exception e){
                throw new EvalException("DOMAIN_ERROR: invalid date/time for mask");
            }
        }
        static String detectCommonDateMask(String s1, String s2){
            for (String m : SUPPORTED_INPUT_MASKS_ORDER){
                try {
                    parseToDateTimeWith(m, s1);
                    parseToDateTimeWith(m, s2);
                    return m;
                } catch (Exception ignored){}
            }
            return null;
        }
        static LocalDateTime addInterval(LocalDateTime dt, int amount, String unit){
            switch(unit){
                case "year": case "years": return dt.plusYears(amount);
                case "month": case "months": return dt.plusMonths(amount);
                case "day": case "days": return dt.plusDays(amount);
                default: throw new EvalException("DOMAIN_ERROR: DateField must be year|month|day");
            }
        }

        // ----------------- Phone mask -----------------
        static String maskPhone(String input, String mask){
            // Only support '(###) ###-####' per spec
            String digits = input.replaceAll("\\D+", "");
            int needed = countChars(mask, '#');
            if (digits.length() != needed)
                throw new EvalException("DOMAIN_ERROR: phone digits ("+digits.length()+") != placeholders ("+needed+")");
            StringBuilder out = new StringBuilder();
            int di = 0;
            for (int i=0;i<mask.length();i++){
                char c = mask.charAt(i);
                if (c=='#') out.append(digits.charAt(di++));
                else out.append(c);
            }
            return out.toString();
        }
        static int countChars(String s, char ch){ int c=0; for (int i=0;i<s.length();i++) if (s.charAt(i)==ch) c++; return c; }

        // ----------------- Number to words (English) -----------------
        static final String[] smalls = {"zero","one","two","three","four","five","six","seven","eight","nine",
                "ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen"};
        static final String[] tens = {"","","twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"};
        static final String[] thousands = {"","thousand","million","billion","trillion"};

        static String englishWords(BigInteger n){
            if (n.equals(BigInteger.ZERO)) return "zero";
            String sign = "";
            if (n.signum() < 0){ sign="minus "; n = n.negate(); }
            List<String> parts = new ArrayList<>();
            int idx=0;
            while (n.compareTo(BigInteger.ZERO) > 0 && idx < thousands.length){
                BigInteger[] qr = n.divideAndRemainder(BigInteger.valueOf(1000));
                int chunk = qr[1].intValue();
                if (chunk != 0){
                    String w = three(chunk);
                    if (!thousands[idx].isEmpty()) w += " " + thousands[idx];
                    parts.add(w);
                }
                n = qr[0]; idx++;
            }
            Collections.reverse(parts);
            return (sign + String.join(" ", parts)).replaceAll(" +", " ").trim();
        }
        static String three(int n){
            StringBuilder sb = new StringBuilder();
            if (n >= 100){ sb.append(smalls[n/100]).append(" hundred"); n %= 100; if (n != 0) sb.append(" "); }
            if (n >= 20){ sb.append(tens[n/10]); if (n%10 != 0) sb.append("-").append(smalls[n%10]); }
            else if (n > 0){ sb.append(smalls[n]); }
            return sb.toString();
        }

        // ----------------- Lookup CSV -----------------
        static final class LookupTableCache {
            private static final Map<String, LookupTable> cache = new HashMap<>();
            static LookupTable get(String name){ return cache.get(name); }
            static void put(String name, LookupTable t){ cache.put(name, t); }
        }
        static final class LookupTable {
            final String name;
            final int pkIndex;          // 1-based
            final boolean hasHeader;
            final boolean trimCells;
            final boolean caseSensitive;
            final Map<String, String[]> rowsByKey; // normalized key -> row

            private LookupTable(String name, int pkIndex, boolean hasHeader, boolean trimCells, boolean caseSensitive, Map<String,String[]> rowsByKey){
                this.name = name; this.pkIndex = pkIndex; this.hasHeader = hasHeader; this.trimCells = trimCells; this.caseSensitive = caseSensitive; this.rowsByKey = rowsByKey;
            }

            static LookupTable load(String tableName){
                Properties cfg = loadConfig(tableName);
                int pkIdx = parseIntProp(cfg, "primaryKeyColumnIndex", true);
                boolean hasHeader = parseBoolProp(cfg, "hasHeader", true);
                boolean trimCells = parseBoolProp(cfg, "trimCells", true);
                boolean caseSensitive = parseBoolProp(cfg, "caseSensitive", true);

                List<String[]> lines = readCsv(tableName);
                if (lines.isEmpty()) throw new EvalException("DOMAIN_ERROR: Lookup '" + tableName + "' is empty");

                int startRow = hasHeader ? 1 : 0;
                if (pkIdx < 1) throw new EvalException("DOMAIN_ERROR: primaryKeyColumnIndex must be >= 1");

                Map<String,String[]> map = new LinkedHashMap<>();
                for (int i = startRow; i < lines.size(); i++){
                    String[] row = lines.get(i);
                    if (pkIdx > row.length) continue;
                    String key = row[pkIdx - 1];
                    if (key == null) continue;
                    String normKey = trimCells ? key.trim() : key;
                    if (!caseSensitive) normKey = normKey.toLowerCase(Locale.ROOT);
                    if (!map.containsKey(normKey)) map.put(normKey, row); // keep first
                }
                return new LookupTable(tableName, pkIdx, hasHeader, trimCells, caseSensitive, map);
            }

            String[] findRow(String inputKey){
                String k = trimCells ? inputKey.trim() : inputKey;
                if (!caseSensitive) k = k.toLowerCase(Locale.ROOT);
                return rowsByKey.get(k);
            }

            private static Properties loadConfig(String tableName){
                String base = "LookupConfigs/" + tableName + ".properties";
                Properties p = new Properties();
                try (InputStream in = ExprEval.class.getResourceAsStream("/" + base)) {
                    if (in != null) { p.load(in); return p; }
                } catch (IOException ignored){}
                File f = new File(base);
                if (f.isFile()) {
                    try (InputStream in = new FileInputStream(f)) { p.load(in); return p; } catch (IOException e) {
                        throw new EvalException("PARSE_ERROR: cannot read " + base);
                    }
                }
                throw new EvalException("DOMAIN_ERROR: missing lookup config " + base);
            }

            private static int parseIntProp(Properties p, String key, boolean required){
                String v = p.getProperty(key);
                if (v == null) {
                    if (required) throw new EvalException("DOMAIN_ERROR: missing config key '" + key + "'");
                    return 0;
                }
                try { return Integer.parseInt(v.trim()); } catch (Exception e){ throw new EvalException("DOMAIN_ERROR: bad integer for '" + key + "'"); }
            }
            private static boolean parseBoolProp(Properties p, String key, boolean def){
                String v = p.getProperty(key);
                if (v == null) return def;
                return Boolean.parseBoolean(v.trim());
            }

            private static List<String[]> readCsv(String tableName){
                String base = "Lookups/" + tableName + ".csv";
                try (InputStream in = ExprEval.class.getResourceAsStream("/" + base)) {
                    if (in != null) return readAllCsvLines(in);
                } catch (IOException ignored){}
                File f = new File(base);
                if (f.isFile()) {
                    try (InputStream in = new FileInputStream(f)) { return readAllCsvLines(in); } catch (IOException e) {
                        throw new EvalException("PARSE_ERROR: cannot read " + base);
                    }
                }
                throw new EvalException("DOMAIN_ERROR: missing lookup table " + base);
            }

            private static List<String[]> readAllCsvLines(InputStream in) throws IOException {
                List<String[]> rows = new ArrayList<>();
                BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                String line;
                while ((line = br.readLine()) != null){
                    rows.add(parseCsvLine(line));
                }
                return rows;
            }

            // Minimal CSV parser: comma-separated; double-quoted with "" escapes
            private static String[] parseCsvLine(String line){
                List<String> cells = new ArrayList<>();
                StringBuilder sb = new StringBuilder();
                boolean inQuotes = false;
                for (int i=0;i<line.length();i++){
                    char c = line.charAt(i);
                    if (inQuotes){
                        if (c=='"'){
                            if (i+1<line.length() && line.charAt(i+1)=='"'){ sb.append('"'); i++; }
                            else { inQuotes=false; }
                        } else { sb.append(c); }
                    } else {
                        if (c=='"'){ inQuotes=true; }
                        else if (c==','){ cells.add(sb.toString()); sb.setLength(0); }
                        else { sb.append(c); }
                    }
                }
                cells.add(sb.toString());
                return cells.toArray(new String[0]);
            }
        }

    } // end StandardFunctions
}
