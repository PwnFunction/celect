function kw(word) {
  return new RegExp(word, 'i');
}

module.exports = grammar({
  name: 'sql',

  extras: $ => [/\s/, /\n/],

  rules: {
    source_file: $ => seq($._statement, optional(';')),

    _statement: $ => choice(
      $.select_statement
    ),

    select_statement: $ => seq(
      kw('SELECT'),
      $.select_list,
      kw('FROM'),
      $.file_name,
      optional($.where_clause),
      optional($.limit_clause),
      optional($.offset_clause)
    ),

    select_list: $ => choice(
      $.column_list,
      '*'
    ),

    column_list: $ => seq(
      $.select_expression,
      repeat(seq(',', $.select_expression))
    ),

    select_expression: $ => choice(
      $.aggregate_function,
      $.column_name,
      seq('(', $.column_name, ')')  // Allow parenthesized column names
    ),
    
    aggregate_function: $ => choice(
      seq(kw('COUNT'), '(', '*', ')'),
      seq(kw('COUNT'), '(', $.column_name, ')')
    ),

    column_name: $ => $._identifier,

    file_name: $ => choice(
      $._identifier,
      $.string_literal
    ),

    where_clause: $ => seq(
      kw('WHERE'),
      $.expression
    ),

    limit_clause: $ => seq(
      kw('LIMIT'),
      $.number_literal
    ),

    offset_clause: $ => seq(
      kw('OFFSET'),
      $.number_literal
    ),

    expression: $ => $.or_expression,

    or_expression: $ => prec.left(1, choice(
      $.and_expression,
      seq($.and_expression, kw('OR'), $.or_expression)
    )),

    and_expression: $ => prec.left(2, choice(
      $.not_expression,
      seq($.not_expression, kw('AND'), $.and_expression)
    )),

    not_expression: $ => choice(
      seq(kw('NOT'), $.not_expression),
      $.primary_expression
    ),

    primary_expression: $ => choice(
      $.comparison_expression,
      $.column_name,
      $.literal,
      seq('(', $.expression, ')')
    ),

    comparison_expression: $ => choice(
      prec.left(3, seq($.primary_expression, '=', $.primary_expression)),
      prec.left(3, seq($.primary_expression, '!=', $.primary_expression)),
      prec.left(3, seq($.primary_expression, '<>', $.primary_expression)),
      prec.left(3, seq($.primary_expression, '>', $.primary_expression)),
      prec.left(3, seq($.primary_expression, '>=', $.primary_expression)),
      prec.left(3, seq($.primary_expression, '<', $.primary_expression)),
      prec.left(3, seq($.primary_expression, '<=', $.primary_expression))
    ),

    literal: $ => choice(
      $.string_literal,
      $.number_literal,
      $.boolean_literal,
      kw('NULL')
    ),

    string_literal: $ => choice(
      seq("'", /[^']*/, "'"),
      seq('"', /[^"]*/, '"')
    ),

    number_literal: $ => /-?\d+(\.\d+)?/,

    boolean_literal: $ => choice(kw('true'), kw('false')),

    _identifier: $ => /[a-zA-Z_][a-zA-Z0-9_]*/
  }
});

