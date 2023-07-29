package au.csiro.pathling.views;

/**
 * The select stanza defines the actual content of the view itself. This stanza is a list where each
 * item in is one of:
 * <ul>
 * <li>a structure with the column name, expression, and optional description,</li>
 * <li>a 'from' structure indicating a relative path to pull fields from in a nested select, or</li>
 * <li>a 'forEach' structure, unrolling the specified path and creating a new row for each item.</li>
 * </ul>
 * See the comments below for details on the semantics.
 *
 * @author John Grimes
 */
public class SelectClause {

}
