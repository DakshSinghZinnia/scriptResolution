# ExprEval

A lightweight Java expression evaluator for XML-backed variables and a small function library.

This repo contains a single-file program `ExprEval.java` that parses and evaluates expressions like:

```
Concat('Hi ', If(A/@B=='X','Yes','No'))
```

It can read variables from an XML file and supports running either via CLI or a simple **resources mode** (no args).

---

## Quick start

### Option 1 — Resources mode (no arguments)
Place these files in a `resources/` folder next to where you run Java:
```
resources/
  ├─ script.txt   # the expression (raw, quotes optional)
  └─ data.xml     # the input XML
```

Run:
```bash
javac ExprEval.java
java ExprEval
```

It will print the evaluated value. The program first tries to load classpath resources `/resources/script.txt` and `/resources/data.xml`, and if not found, falls back to the filesystem paths `resources/script.txt` and `resources/data.xml`.

### Option 2 — CLI mode
```bash
javac ExprEval.java
java -cp . ExprEval -xml <path-to-xml> "<expression>"
```

Examples:
```bash
java ExprEval -xml ~/Downloads/data.xml "Concat('Hi ', If(A/@B=='X','Yes','No'))"
java ExprEval -xml data.xml "UpperCase('abc')"
java ExprEval -xml data.xml "Add(2, 3) * 4"
```

> **Tip (macOS/Linux):** You can also run in “source-file mode” (Java 11+):
> ```bash
> java ExprEval.java -xml data.xml "Concat('Hi ', If(A/@B=='X','Yes','No'))"
> ```

---

## What the language supports

### Types
- **String**, **Numeric** (base 10; integers and real numbers), **Boolean**.
- Single characters are strings.
- Mixed-type arithmetic is **not** allowed. Equality requires same types.

### Operators
- Arithmetic: `+ - * / %`  
  `%` requires integer operands; `/` returns a real number.
- Comparison: `< <= > >= == !=` (for numbers or strings; booleans only for `==`/`!=`).
- Logical: `&& ||` (both sides are evaluated; **no short-circuit**).
- Unary: `-` (numeric only).

Operator precedence and associativity are standard (`*`/`/`/`%` bind tighter than `+`/`-`, then comparisons, then `&&`, then `||`).

### Strings
- Use single `'` or double `"` quotes.
- Supports escapes: `\\ \' \" \n \r \t \uXXXX`.
- HTML entities in the expression are decoded before lexing: `&lt; &gt; &amp; &quot; &apos;`.

### Variables (XML-backed)
- Path shape: `Element[/Element...][/@Attribute]`
- The value of a terminal **element** step is the element’s text (concatenated text/CDATA).
- **Case-sensitive** names for elements/attributes.

**Attribute→element fallback:** if you ask for `A/@B` but `A` has no attribute `B`, we optionally read the child element `<B>`’s text instead. This is enabled by default and controlled by:

```java
XmlUtil.LENIENT_ATTR_FALLBACK = true;
```

Set to `false` if you want strict behavior.

#### Examples
Given this XML:
```xml
<A>
  <B>X</B>
</A>
```
- `If(A/@B=='X','Yes','No')` → with fallback enabled → `Yes`  
- `If(A/B=='X','Yes','No')`  → always works → `Yes`

---

## Built-in functions (case-insensitive names)

### Conditional
- `If(Boolean, Then, Else)` → returns either branch (types may differ).

### Strings
- `Concat(a, b, ...)`
- `UpperCase(s)`, `LowerCase(s)`, `TitleCase(s)`
- `Replace(input, search, replace)`
- `Trim(input, trimChars)` — trims any of the characters in `trimChars` from both ends
- `Length(s)`
- `Insert(input, insertStr, atIndex)`
- `IndexOf(input, search [, startIndex])`
- `LastIndexOf(input, search [, startIndex])`
- `SubString(input, start [, length])`
- `SubStringBefore(input, sep)`, `SubStringAfter(input, sep)`

### Numeric / Math
- `Add(x, y)`, `Subtract(x, y)`, `Multiply(x, y)`, `Divide(x, y)`, `Mod(x, y)`
- `Abs(x)`, `Floor(x)`, `Ceil(x)`, `Round(x)`  
  > `Round(x)` currently rounds to 0 decimal places (HALF_UP). Adjust if your catalog specifies a scale.

### Formatting / Masking
- `MaskNumber(number, pattern)` — uses Java `DecimalFormat`
- `MaskPhoneNumber(digitsOrMixed, mask)` — `#` consumes digits
- `MaskDateTime(inputDateTime, outPattern)`

### Numbers ↔ Words
- `NumberToWords(integer)` — English words up to trillions

### Date/Time
- `ToDateTime(input, inPattern)` → returns `"yyyy-MM-dd'T'HH:mm:ss"`
- `AddInterval(dateTime, amount, unit)`
- `SubtractInterval(dateTime, amount, unit)`
- `Duration(dateTimeA, dateTimeB, unit)`

Supported units: `YEAR(S)`, `MONTH(S)`, `DAY(S)`, `HOUR(S)`, `MINUTE(S)`, `SECOND(S)`.

> Date/time parsing accepts: `yyyy-MM-dd'T'HH:mm:ss`, `yyyy-MM-dd HH:mm:ss`, `yyyy-MM-dd`, and Java’s `ISO_LOCAL_DATE_TIME`.

### Case sensitivity
- **Functions:** case-insensitive (e.g., `concat`, `Concat`, `CONCAT` are all fine).
- **Variables / operators:** case-sensitive.

---

## Errors

The evaluator throws a short error message and exits with code 2. Common messages:
- `PARSE_ERROR: ...`
- `TYPE_ERROR: ...`
- `ARITY_MISMATCH: 'Fn' expects N args`
- `DIVIDE_BY_ZERO`
- `RANGE_ERROR: ...`
- `DOMAIN_ERROR: ...`
- `UNKNOWN_FUNCTION: 'Name'`

Examples:
```bash
# Unknown function
java ExprEval -xml data.xml "Foo(1)"
# → UNKNOWN_FUNCTION: 'Foo'
```

---

## Project layout (suggested)

```
.
├─ ExprEval.java
├─ resources/
│  ├─ script.txt
│  └─ data.xml
└─ README.md
```

`script.txt` contains the raw expression (you may include or omit outer quotes).

---

## Example

**resources/data.xml**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<A>
  <B>X</B>
</A>
```

**resources/script.txt**
```
Concat('Hi ', If(A/@B=='X','Yes','No'))
```

Run:
```bash
javac ExprEval.java
java ExprEval
# Output:
# Hi Yes
```

If you disable the fallback:
```java
// inside XmlUtil
static final boolean LENIENT_ATTR_FALLBACK = false;
```
the same script with the same XML prints:
```
Hi No
```

---

## Building a JAR (optional)

```bash
# Compile
javac ExprEval.java

# Create a simple jar
jar --create --file expreval.jar ExprEval.class

# Run with classpath
java -cp expreval.jar ExprEval -xml resources/data.xml "Concat('Hi ', If(A/@B=='X','Yes','No'))"
```

If you later introduce a package, compile with `-d out` and run using the fully-qualified name.

---

## Troubleshooting

- **Could not find or load main class / wrong name:**  
  Ensure there is **no `package`** line in `ExprEval.java` (or run with the fully qualified name and correct classpath root).

- **Resources not loaded:**  
  Make sure `resources/script.txt` and `resources/data.xml` exist in the working directory, or put them on the classpath at `/resources/script.txt` and `/resources/data.xml`.

- **Paths with spaces:**  
  Quote the `-xml` path: `-xml "$HOME/Downloads/My Data/data.xml"`

- **Got `Hi No` instead of `Hi Yes`:**  
  You’re querying an attribute (`@B`) but your XML has an element `<B>`. With the default fallback, it should still yield `Yes`. If you turned strict mode on, change the script to use `A/B` or update the XML to `A B="..."`.

---

## Requirements

- Java 11+ (tested with HotSpot/OpenJDK on macOS).

---

## Notes

- Logical operators evaluate **both** sides (no short-circuit). If you need short-circuit semantics, this can be changed.
- Only the listed functions are available; names are case-insensitive.
- Variables/operators are case-sensitive.
- All numbers are base 10.

---

## License

Choose a license that suits your project (e.g., MIT). Add it as `LICENSE` in the repo.
