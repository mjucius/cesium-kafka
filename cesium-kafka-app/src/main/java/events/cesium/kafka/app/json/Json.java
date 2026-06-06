package events.cesium.kafka.app.json;

import java.util.Locale;
import org.jspecify.annotations.Nullable;

/**
 * A tiny streaming JSON writer for the fixed-shape objects the observability endpoints and the JSON
 * log encoder emit (design §9). Not a general-purpose serializer: it offers just object and array
 * builders with correct string escaping, enough to render the health/info payloads and a compact log
 * line without a binding framework on these paths.
 *
 * <p>Numbers are emitted verbatim, strings are RFC 8259-escaped (control characters below {@code
 * U+0020} become {@code \\uXXXX}), and {@code null} string values render as the JSON literal {@code
 * null}. Builders are single-use and not thread-safe; each request handler builds its own.
 */
public final class Json {

    private Json() {}

    /** Starts a new JSON object builder. */
    public static Obj object() {
        return new Obj();
    }

    /** Starts a new JSON array builder. */
    public static Arr array() {
        return new Arr();
    }

    /** Returns {@code value} as a quoted, escaped JSON string, or the literal {@code null}. */
    public static String quote(@Nullable String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 2);
        escapeInto(sb, value);
        return sb.toString();
    }

    private static void escapeInto(StringBuilder sb, String value) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** A JSON object builder: chain {@code field}/{@code raw} calls, then {@link #end()}. */
    public static final class Obj {
        private final StringBuilder sb = new StringBuilder("{");
        private boolean first = true;

        private Obj() {}

        private Obj key(String key) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            escapeInto(sb, key);
            sb.append(':');
            return this;
        }

        /** Appends a string-valued field ({@code null} renders as the JSON literal {@code null}). */
        public Obj str(String key, @Nullable String value) {
            return key(key).appendRaw(Json.quote(value));
        }

        /** Appends an integral-valued field. */
        public Obj num(String key, long value) {
            return key(key).appendRaw(Long.toString(value));
        }

        /** Appends a boolean-valued field. */
        public Obj bool(String key, boolean value) {
            return key(key).appendRaw(Boolean.toString(value));
        }

        /** Appends a field whose value is already-rendered JSON (a nested object or array). */
        public Obj raw(String key, String json) {
            return key(key).appendRaw(json);
        }

        private Obj appendRaw(String json) {
            sb.append(json);
            return this;
        }

        /** Closes the object and returns its JSON text. The builder must not be reused after this. */
        public String end() {
            return sb.append('}').toString();
        }
    }

    /** A JSON array builder: add already-rendered element JSON, then {@link #end()}. */
    public static final class Arr {
        private final StringBuilder sb = new StringBuilder("[");
        private boolean first = true;

        private Arr() {}

        /** Appends an element whose value is already-rendered JSON. */
        public Arr value(String json) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(json);
            return this;
        }

        /** Appends a string element. */
        public Arr str(@Nullable String value) {
            return value(Json.quote(value));
        }

        /** Closes the array and returns its JSON text. The builder must not be reused after this. */
        public String end() {
            return sb.append(']').toString();
        }
    }
}
