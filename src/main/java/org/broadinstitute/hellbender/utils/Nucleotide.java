package org.broadinstitute.hellbender.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

/**
 * Represents the nucleotide alphabet with support for IUPAC ambiguity codes.
 *
 * <p>
 *    This enumeration not only contains concrete nucleotides, but also
 *    values to represent ambiguous and invalid codes.
 * </p>
 *
 * @author Valentin Ruano-Rubio &lt;valentin@broadinstitute.org&gt;
 */
public enum Nucleotide {
    A(true, false, false, false),
    C(false, true, false, false),
    G(false, false, true, false),
    T(false, false, false, true),
    R(true, false, true, false),
    Y(false, true, false, true),
    S(false, true, true, false),
    W(true, false, false, true),
    K(false, false, true, true),
    M(true, false, true, false),
    B(false, true, true, true),
    D(true, false, true, true),
    H(true, true, false, true),
    V(true, true, true, false),
    N(true, true, true, true),
    X(false, false, false, false);

    public static final Nucleotide U = T;
    public static final Nucleotide INVALID = X;

    public static final List<Nucleotide> REGULAR_BASES = Arrays.asList(A, C, G, T);

    private static final Nucleotide[] baseToValue = new Nucleotide[1 << Byte.SIZE];
    private static final Nucleotide[] maskToValue = new Nucleotide[1 << 4];
    private static final byte INVALID_ENCODING = 'X';

    private static final Nucleotide[] reverseComplement = new Nucleotide[Byte.MAX_VALUE];

    static {
        Arrays.fill(baseToValue, X);
        for (final Nucleotide nucleotide : values()) {
            baseToValue[nucleotide.lowerCaseByteEncoding] = baseToValue[nucleotide.upperCaseByteEncoding] = nucleotide;
            maskToValue[nucleotide.acgtMask] = nucleotide;
        }
        baseToValue['u'] = baseToValue['U'] = U;
        baseToValue['x'] = baseToValue['X'] = X;
        baseToValue['n'] = baseToValue['N'] = N;

        Arrays.fill(reverseComplement, INVALID);
        reverseComplement['a'] = reverseComplement['A'] = T;
        reverseComplement['c'] = reverseComplement['C'] = G;
        reverseComplement['g'] = reverseComplement['G'] = C;
        reverseComplement['t'] = reverseComplement['T'] = A;
        reverseComplement['u'] = reverseComplement['U'] = A;
        reverseComplement['x'] = reverseComplement['X'] = X;
        reverseComplement['n'] = reverseComplement['N'] = N;
    }

    private static final int A_MASK = 1;
    private static final int C_MASK = A_MASK << 1;
    private static final int G_MASK = C_MASK << 1;
    private static final int T_MASK = G_MASK << 1;

    private final int acgtMask;
    private final boolean isConcrete;

    /**
     * Holds lower-case byte encoding for this nucleotide; {@code 0} for {@link Nucleotide#INVALID}.
     */
    private final byte lowerCaseByteEncoding;

    /**
     * Holds the upper-case byte encoding for this nucleotide; {@code 0} for {@link Nucleotide#INVALID}.
     */
    private final byte upperCaseByteEncoding;

    Nucleotide(final boolean a, final boolean c, final boolean g, final boolean t) {
        acgtMask = (a ? A_MASK : 0) | (c ? C_MASK : 0) | (g ? G_MASK : 0) | (t ? T_MASK : 0);
        isConcrete = acgtMask == A_MASK || acgtMask == C_MASK || acgtMask == G_MASK || acgtMask == T_MASK;
        lowerCaseByteEncoding = (byte) Character.toLowerCase(name().charAt(0));
        upperCaseByteEncoding = (byte) Character.toUpperCase(name().charAt(0));
    }

    /**
     * Returns the base that corresponds to this nucleotide.
     * <p>
     *    The base is returned in uppercase.
     * </p>
     * <p>
     *     The {@link #INVALID} nucleotide does not have an actual base then resulting in an exception.
     * </p>
     * @return a valid byte representation for a nucleotide, {@code 0} for {@link Nucleotide#INVALID}.
     */
    public byte encodeAsByte(final boolean lowerCase) {
        return lowerCase ? lowerCaseByteEncoding : upperCaseByteEncoding;
    }

    /**
     * Returns the nucleotide encoding in a byte using its upper-case representation.
     * @return a valid upper-case byte representation for a nucleotide, {@code 0} for {@link Nucleotide#INVALID}.
     */
    public byte encodeAsByte() {
        return upperCaseByteEncoding;
    }

    /**
     * Returns the nucleotide that corresponds to a particular {@code byte} typed base code.
     * @param base the query base code.
     * @throws IllegalArgumentException if {@code base} is negative.
     * @return never {@code null}, but {@link #INVALID} if the base code does not
     * correspond to a valid nucleotide specification.
     */
    public static Nucleotide decode(final byte base) {
        return baseToValue[Utils.validIndex(base, baseToValue.length)];
    }

    /**
     * Checks whether the nucleotide refer to a concrete (rather than ambiguous) base.
     * @return {@code true} iff this is a concrete nucleotide.
     */
    public boolean isConcrete() {
        return isConcrete;
    }

    /**
     * Checks whether the nucleotide refer to an ambiguous base.
     * @return {@code true} iff this is an ambiguous nucleotide.
     */
    public boolean isAmbiguous() {
        return !isConcrete && this != INVALID;
    }

    public boolean includes(final Nucleotide other) {
        Utils.nonNull(other);
        if (this == INVALID || other == INVALID) {
            return false;
        } else {
            return ((this.acgtMask & other.acgtMask) == other.acgtMask);
        }
    }

    /**
     * Checks whether to base encodings make reference to the same {@link #Nucleotide}
     *  instance regardless of their case.
     * <p>
     *     This method is a shorthard for:
     *     <pre>{@link #decode}(a){@link #same(Nucleotide) same}({@link #decode}(b)) </pre>.
     * </p>
     *
     *  <p>
     *      The order of the inputs is not relevant, therefore {@code same(a, b) == same(b, a)} for any
     *      given {@code a} and {@code b}.
     *  </p>
     *  <p>
     *      Notice that if either or both input bases make reference to an invalid nucleotide (i.e. <pre> {@link #decode}(x) == {@link #INVALID}},
     *      this method will return {@code false} even if {@code a == b}.
     *  </p>
     * @param a the first base to compare (however order is not relevant).
     * @param b the second base to compare (however order is not relevant).
     * @return {@code true} iff {@code {@link #decode}}.same({@link #decode}(b))}}
     */
    public static boolean same(final byte a, final byte b) {
        return baseToValue[a] == baseToValue[b] && baseToValue[a] != INVALID;
    }

    /**
     * Checks whether this and another {@link #Nucleotide} make reference to the same nucleotide(s).
     * <p>
     *     In contrast with {@link #equals}, this method will return {@code false} if any of the two, this
     *     or the input nucleotide is the {@link #INVALID} enum value. So even <pre>{@link #INVALID}.same({@link #INVALID})</pre>
     *     will return {@code null}.
     * </p>
     *
     * @param other the other nucleotide.
     * @return {@code true} iff this and the input nucleotide make reference to the same nucleotides.
     */
    public boolean same(final Nucleotide other) {
        if (this == INVALID || other == INVALID) {
            return false;
        } else {
            return this == other;
        }
    }

    public boolean intersects(final Nucleotide other) {
        Utils.nonNull(other);
        return ((this.acgtMask & other.acgtMask) != 0);
    }

    /**
     * Returns the nucleotide code that corresponds to all the nucleotides that are compatible with
     * this and another nucleotide code.
     * <p>
     * Notice that this method will return {@link #INVALID} if the intersection is empty.
     * </p>
     *
     * @param other the other nucleotide to intersect with.
     * @return never {@code null}.
     */
    public Nucleotide intersection(final Nucleotide other) {
        Utils.nonNull(other);
        return maskToValue[this.acgtMask & other.acgtMask];
    }

    public boolean isComplementOf(final Nucleotide other) {
        switch (this) {
            case A: return other == T;
            case C: return other == G;
            case G: return other == C;
            case T: return other == A;
            default:
                return false;
        }
    }

    /**
     * Returns the complement nucleotide code for this one.
     * <p>
     *     For ambiguous nucleotide codes, this will return the ambiguous code that encloses the complement of
     *     each possible nucleotide in this code.
     * </p>
     * <p>
     *     The complement of the {@link #INVALID} nucleotide its itself.
     * </p>
     * @return never {@code null}.
     */
    public Nucleotide complement() {
        switch (this) {
            case A: return T;
            case C: return G;
            case G: return C;
            case T: return A;
            case X:
            case N:
            case S:
            case W:
                    return this;
            case R: return Y;
            case Y: return R;
            case M: return K;
            case K: return M;
            case H: return D;
            case D: return H;
            case B: return V;
            case V: return B;
            default:
                throw new IllegalArgumentException("unsupported value " + this);
        }
    }

    /**
     * Returns the complement for a base code
     * @param b
     * @return
     */
    public static byte complement(final byte b) {

        final Nucleotide value = decode(b);
        final Nucleotide compl = value.complement();
        return compl.encodeAsByte(Character.isLowerCase(b));
    }

    /**
     * Helper class to count the number of occurrences of each nucleotide code in
     * a sequence.
     */
    public static class Counter {

        private final long[] counts;

        /**
         * Creates a new counter with all counts set to 0.
         */
        public Counter() {
            counts = new long[Nucleotide.values().length];
        }

        /**
         * Increases by 1 the count for a nucleotide.
         * @param nucleotide the target nucleotide.
         * @throws IllegalArgumentException if nucleotide is {@code null}.
         */
        public void add(final Nucleotide nucleotide) {
            counts[Utils.nonNull(nucleotide).ordinal()]++;
        }

        /**
         * Increases the nucleotide that corresponds to the input base own count by 1.
         * @param base the base code.
         * @throws IllegalArgumentException if {@code base} is {@code negative}.
         */
        public void add(final byte base) {
            add(decode(base));
        }

        /**
         * Returns the current count for a given nucleotide.
         * @param nucleotide the query nucleotide.
         * @throws IllegalArgumentException if {@code nucleotide} is {@code null}.
         * @return 0 or greater.
         */
        public long get(final Nucleotide nucleotide) {
            return counts[Utils.nonNull(nucleotide).ordinal()];
        }

        /**
         * Increase by one the count for a nucleotide for each
         * occurrence of such in the input byte array base codes.
         * @param bases the input base codes.
         * @throws IllegalArgumentException if {@code bases} are null or
         * it contains negative values.
         */
        public void addAll(final byte[] bases) {
            Utils.nonNull(bases);
            for (final byte base : bases) {
                add(base);
            }
        }

        /**
         * Reset all the counts to 0.
         */
        public void clear() {
            Arrays.fill(counts, 0);
        }

        public long sum() {
            return LongStream.of(counts).sum();
        }
    }

    public Nucleotide transition() {
        switch(this) {
            case A:
                return G;
            case G:
                return A;
            case C:
                return T;
            case T:
                return C;
            case X:
            case N:
            case R:
            case Y:
                return this;
            case S:
                return W;
            case W:
                return S;
            case M:
                return K;
            case K:
                return M;
            case B:
                return H;
            case H:
                return B;
            case D:
                return V;
            case V:
                return D;
            default:
                throw new IllegalStateException("unexpected nucleotide " + this);
        }
    }

    public Nucleotide transversion() {
        switch (this) {
            case A:
            case G:
                return Y;
            case C:
            case T:
                return R;
            case N:
            case X:
                return this;
            case Y:
                return R;
            case R:
                return Y;
            default: // any other ambiguous will include all possible bases.
                return N;
        }
    }

    /**
     * Transvertion mutation toward a strong or a weak base.
     * <p>
     *     This method provides a non-ambiguous alternative to {@link #transversion()} for
     *     concrete nucleotides.
     * </p>
     *
     * @param toStrong whether the result should be a strong ({@code S: G, C}) or weak ({@code W: A, T}) nucleotide(s).
     * @return nucleotides that may emerged from such a transversion.
     */
    public Nucleotide transversion(final boolean toStrong) {
        switch (this) {
            case A:
            case G:
            case R:
                return toStrong ? C : T;
            case C:
            case T:
            case Y:
                return toStrong ? G : A;
            case X:
                return this;
            default:
                return toStrong ? S : W;
        }
    }
}
