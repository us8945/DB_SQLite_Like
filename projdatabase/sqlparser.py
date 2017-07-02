'''
Created on Jul 10, 2016

@author: Kevin Manley
'''

"""
Modified from this version:
See links for example yacc SQL grammars:
https://github.com/kmanley/redisql/blob/master/sqlparser.py
# TODO: support select fname + ' ' + lname from people
see grammar above
# TODO: break sqlparser into its own file, have it instantiate AstNodes via
a factory, so a client of sqlparser can customize (derive from AstNode) and
have custom actions
"""
from ply import lex, yacc

class SqlLexer(object):
    rserved=['INSERT',
             'UPDATE',
             'DROP',
             'SET',
            'INTO',
            'SELECT',
            'FROM',
            'WHERE',
            'ORDER',
            'BY',
            'VALUES',
            'AND',
            'OR',
            'NOT',
            'SHOW',
            'TABLE',
            'TABLES',
            'CREATE',
            'PRIMARY',
            'KEY',
             'NULL',
            'DELETE',
            'INT',
            'DOUBLE',
            'TEXT',
            'TINYINT',
            'SMALLINT',
            'BIGINT',
            'REAL',
            'DATETIME',
            'DATE',
                    ]

    reserved = dict([(token, token) for token in rserved]) 
    
    tokens = ['NUMBER',
              'FLOAT',
              'ID', 
              'STRING',
              'COMMA',      'SEMI',
              'PLUS',       'MINUS',
              'TIMES',      'DIVIDE',
              'LPAREN',     'RPAREN',
              'GT',         'GE',
              'LT',         'LE',
              'EQ',         'NE', 
              ] + list(reserved.values())
    
    def t_FLOAT(self,t):
        r'(\+|\-)?\d*\.\d*(e-?\d+)?'
        t.value = float(t.value)
        return t
    
    def t_NUMBER(self, t):
        # TODO: see http://docs.python.org/reference/lexical_analysis.html
        # for what Python accepts, then use eval
        r'\d+'
        t.value = int(t.value)    
        return t
    
    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z_0-9]*'
        
        # upper() make reserved words case insensitive
        t.type = SqlLexer.reserved.get(t.value.upper(),'ID')    # Check for reserved words
        # redis is case sensitive in hash keys but we want the sql to be case insensitive,
        # so we lowercase identifiers 
        t.value = t.value.lower()
        return t
    
    def t_STRING(self, t):
        # TODO: unicode...
        # Note: this regex is from pyparsing, 
        # see http://stackoverflow.com/questions/2143235/how-to-write-a-regular-expression-to-match-a-string-literal-where-the-escape-is
        # TODO: may be better to refer to http://docs.python.org/reference/lexical_analysis.html 
        '(?:"(?:[^"\\n\\r\\\\]|(?:"")|(?:\\\\x[0-9a-fA-F]+)|(?:\\\\.))*")|(?:\'(?:[^\'\\n\\r\\\\]|(?:\'\')|(?:\\\\x[0-9a-fA-F]+)|(?:\\\\.))*\')'
        t.value = eval(t.value) 
        #t.value[1:-1]
        return t
        
    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
    t_ignore  = ' \t'
    
    #literals = ['+', '-', '*', '/', '>', '>=', '<', '<=', '=', '!=']
    # Regular expression rules for simple tokens
    t_COMMA   = r'\,'
    t_SEMI    = r';'
    t_PLUS    = r'\+'
    t_MINUS   = r'-'
    t_TIMES   = r'\*'
    t_DIVIDE  = r'/'
    t_LPAREN  = r'\('
    t_RPAREN  = r'\)'
    t_GT      = r'>'
    t_GE      = r'>='
    t_LT      = r'<'
    t_LE      = r'<='
    t_EQ      = r'='
    t_NE      = r'!='
    #t_NE      = r'<>'
    
    def t_error(self, t):
        raise TypeError("Unknown text '%s'" % (t.value,))

    def build(self, **kwargs):
        self.lexer = lex.lex(module=self, **kwargs)
        return self.lexer

    def test(self):
        while True:
            text = input("sql> ").strip()
            if text.lower() == "quit":
                break
            self.lexer.input(text)
            while True:
                tok = self.lexer.token()
                if not tok: 
                    break
                print(tok)
        
# TODO: consider using a more formal AST representation        
#class Node(object):
#    def __init__(self, children=None, leaf=None):
#        if children:
#            self.children = children
#        else:
#            self.children = [ ]
#        self.leaf = leaf
        
class SqlParser(object):
    
    tokens = SqlLexer.tokens
    
    #def p_empty(self, p):
    #    'empty :'
    #    pass    
    precedence = (
        ('left', 'OR'),
        ('left', 'AND'),
        ('left', 'NOT'),
        ('left', 'EQ', 'NE', 'LT', 'GT', 'LE', 'GE'),
        ('left', 'PLUS', 'MINUS'),
        ('left', 'TIMES', 'DIVIDE'),
        )
    
    def p_statement_list(self, p):
        """
        statement_list : statement
                       | statement_list statement
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[2]]
            
    def p_statement(self, p):
        """
        statement : insert_statement
                  | select_statement
                  | show_statement
                  | create_table_statement
                  | delete_statement
                  | update_statement
                  | drop_table_statement
        """
        p[0] = p[1]
    
    def p_show_statement_1(self, p):
        """
        show_statement : SHOW TABLES
                       | SHOW TABLES SEMI
        """
        p[0] = ('show-tables')
        
    def p_show_statement_2(self, p):
        """
        show_statement : SHOW TABLE ID
                       | SHOW TABLE ID SEMI
        """
        p[0] = ('show-table', p[3])
    
    def p_drop_table_statement(self ,p):
        """
        drop_table_statement : DROP TABLE ID
                             | DROP TABLE ID SEMI
        """
        p[0] = ('drop-table', p[3])
        
        
    def p_create_table_statement(self ,p):
        """
        create_table_statement : CREATE TABLE ID LPAREN create_table_column_list RPAREN
                               | CREATE TABLE ID LPAREN create_table_column_list RPAREN SEMI
        """
        p[0] = ('create-table', p[3], p[5])
        
    def p_create_table_column_list(self, p):
        """
        create_table_column_list : create_table_column_spec COMMA create_table_column_list
                                 | create_table_column_spec
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else: # list case
            p[0] = [p[1]] + p[3]
            
    def p_create_table_column_spec_1(self, p):
        """
        create_table_column_spec : ID data_type qualification_list
                                 | ID data_type
        """
        
        if len(p) == 4:
            p[0] = (p[1], p[2], p[3])
        else:
            p[0] = (p[1], p[2], None)
            
    def p_data_type(self, p):
        """
            data_type : INT
                      | DOUBLE
                      | TEXT
                      | TINYINT
                      | SMALLINT
                      | BIGINT
                      | REAL
                      | DATETIME
                      | DATE
        """
        p[0] = p[1]
        
            
    def p_qualification_list(self, p):
        """
        qualification_list : qualification_spec qualification_list  
                           | qualification_spec
        """
        if len(p) == 2:
            p[0] = p[1]
        else: # list case
            p[0] = p[1] + p[2]
            
    def p_qualification_spec_1(self, p):
        """
        qualification_spec : NOT NULL
        """
        p[0] = ['not-null']
            
    def p_qualification_spec_2(self, p):
        """
        qualification_spec : PRIMARY KEY
        """
        p[0] = ['primary-key']
        
        
    def p_delete_statement(self, p):
        """
        delete_statement : DELETE FROM ID opt_where_clause
                         | DELETE FROM ID opt_where_clause SEMI
        """
        p[0] = ('delete', p[3], p[4])
            
    def p_insert_statement_1(self, p):
        # TODO: support extension: insert into X (a,b,c) VALUES (a1,b1,c1), (a2,b2,c2), ...
        """
        insert_statement : INSERT ID LPAREN id_list RPAREN VALUES LPAREN expr_list RPAREN
                         | INSERT ID LPAREN id_list RPAREN VALUES LPAREN expr_list RPAREN SEMI
                         | INSERT INTO ID LPAREN id_list RPAREN VALUES LPAREN expr_list RPAREN 
                         | INSERT INTO ID LPAREN id_list RPAREN VALUES LPAREN expr_list RPAREN SEMI
        """
        if p[2].lower() == "into":
            p[0] = ('insert', p[3], p[5], p[9])
        else:
            p[0] = ('insert', p[2], p[4], p[8])
            
    def p_insert_statement_2(self, p):
        # TODO: support extension: insert into X (a,b,c) VALUES (a1,b1,c1), (a2,b2,c2), ...
        """
        insert_statement : INSERT ID VALUES LPAREN expr_list RPAREN
                         | INSERT ID VALUES LPAREN expr_list RPAREN SEMI
                         | INSERT INTO ID VALUES LPAREN expr_list RPAREN 
                         | INSERT INTO ID VALUES LPAREN expr_list RPAREN SEMI
        """
        if p[2].lower() == "into":
            p[0] = ('insert', p[3], p[6])
        else:
            p[0] = ('insert', p[2], p[5])

    def p_update_statement(self, p):
        """
        update_statement : UPDATE ID SET asignment_list opt_where_clause 
                         | UPDATE ID SET asignment_list opt_where_clause SEMI
        """
        p[0] = ('update', p[2], p[4], p[5])
        
    def p_asignment_list(self,p):
        """
        asignment_list : asignment_spec COMMA asignment_list
                                 | asignment_spec
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else: # list case
            p[0] = [p[1]] + p[3]
    def p_asignment_spec(self,p):
        """
        asignment_spec : ID EQ conditional_expr   
        """
        p[0] = (p[1],str(p[3]))
    
    def p_select_statement(self, p):
        """
        select_statement : SELECT select_columns FROM ID opt_where_clause opt_orderby_clause
                         | SELECT select_columns FROM ID opt_where_clause opt_orderby_clause SEMI
        """
        p[0] = ('select', p[2], p[4], p[5], p[6])
        
    def p_select_columns(self, p):
        """
        select_columns : TIMES
                       | id_list
        """
        p[0] = p[1]
        
    def p_opt_where_clause(self, p):
        """
        opt_where_clause : WHERE conditional_expr
                         |
        """
        if len(p) == 1:
            p[0] = None
        else:
            p[0] = p[2]
            
    def p_conditional_expr(self, p):
        """
        conditional_expr : conditional_expr OR conditional_expr
                         | conditional_expr AND conditional_expr
                         | NOT conditional_expr
                         | LPAREN conditional_expr RPAREN
                         | predicate
        """
        lenp = len(p)
        if lenp == 4:
            if p[1] == '(':
                p[0] = p[2]
            else:
                p[0] = (p[2], p[1], p[3])
        elif lenp == 3:
            p[0] = (p[1], p[2])
        else:
            p[0] = p[1]
            
    # TODO: there are other predicates...see sql2.y            
    def p_predicate(self, p):
        """
        predicate : comparison_predicate
        """
        p[0] = p[1]
        
    def p_comparison_predicate(self, p):
        """
        comparison_predicate : scalar_exp EQ scalar_exp
                             | scalar_exp NE scalar_exp
                             | scalar_exp LT scalar_exp
                             | scalar_exp LE scalar_exp
                             | scalar_exp GT scalar_exp
                             | scalar_exp GE scalar_exp
                             | scalar_exp
        """
        if len(p) ==4:
            p[0] = (p[2], p[1], p[3])
        else:
            p[0] = p[1]
        
    # TODO: unify this with old expr rules
    def p_scalar_exp(self, p):
        """
        scalar_exp : scalar_exp PLUS scalar_exp
                   | scalar_exp MINUS scalar_exp
                   | scalar_exp TIMES scalar_exp
                   | scalar_exp DIVIDE scalar_exp
                   | atom
                   | LPAREN scalar_exp RPAREN
        """
        lenp = len(p)
        if lenp == 4:
            if p[1] == "(":
                p[0] = p[2]
            else:
                p[0] = (p[2], p[1], p[3])
        elif lenp == 2:
            p[0] = p[1]
        else:
            raise AssertionError()
        
    def p_atom_1(self, p):
        """
        atom : ID
        """
        p[0] = p[1]
    
    def p_atom_2(self, p):
        """
        atom : FLOAT
             | NUMBER
             | STRING
        """
        p[0] = ("val",p[1])
    
    def p_atom_3(self, p):
        """
        atom : NULL
        """
        p[0] = ('val','PROJ2-NULL')
            
    # TODO: more advanced order by including multiple columns, asc/desc            
    def p_opt_orderby_clause(self, p):
        """
        opt_orderby_clause : ORDER BY ID
                           |
        """
        if len(p) == 1:
            p[0] = None
        else:
            p[0] = p[3]
            
    #def p_conditional_expr(self, p):
    #    """
    #    conditional_expr : relational_expr
    #                     | conditional_expr AND conditional_expr
    #                     | conditional_expr OR conditional_expr
    #    """
    #    if len(p) == 2:
    #        p[0] = [p[1]]
    #    else:
    #        p[0] = (p[2], p[1], p[3])
            
    #def p_relational_expr(self, p):
    #    """
    #    relational_expr : expr LT expr
    #                    | expr LE expr
    #                    | expr GT expr
    #                    | expr GE expr
    #                    | expr EQ expr
    #    """
    #    p[0] = (p[2], p[1], p[3])
        
    def p_id_list(self, p):
        """
        id_list : ID
                | id_list COMMA ID
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[3]]

    def p_expr_list(self, p):
        """
        expr_list : conditional_expr
                  | expr_list COMMA conditional_expr
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + [p[3]]

    def p_expr_1(self, p):
        """
        expr : expr PLUS term
             | expr MINUS term
             | term
             | ID
             | STRING
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = ('binop', p[2], p[1], p[3])
    
    def p_expr_2(self, p):
        """
        expr : NULL
             |   
        """
        p[0] = 'PROJ2-NULL'
            
    def p_term(self, p):
        """
        term : term TIMES factor
             | term DIVIDE factor
             | factor
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = ('binop', p[2], p[1], p[3])
            

    def p_factor(self, p):
        """
        factor : NUMBER
               | LPAREN expr RPAREN
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = p[2] 
    
    def p_error(self, p):
        print("DavisDB Syntax error in input") # TODO: at line %d, pos %d!" % (p.lineno)
    
    def build(self, **kwargs):
        self.parser = yacc.yacc(module=self,errorlog=yacc.NullLogger(), **kwargs)
        return self.parser

    def interactive(self):
        lexer = SqlLexer().build()
        while True:
            text = input("davisql> ").strip()
            if text.lower() in ["quit","exit","exit;"]:
                break
            if text:
                result = self.parser.parse(text, lexer=lexer)
                print("parse result -> %s" % result)
    
    def text(self, content, debug=False):
        lexer = SqlLexer().build(debug=debug)
        result = self.parser.parse(content, lexer=lexer, debug=debug)
        return result

def unittest_lexer():
    l = SqlLexer()
    l.build()
    l.test()
        
def interactive_parser():
    p = SqlParser()
    p.build()
    p.interactive()    

def one_time_parser():
    p = SqlParser()
    p.build()
    lexer = SqlLexer().build()
        
if __name__ == "__main__":   
    interactive_parser() 
    ##unittest_lexer()
        
    
    import unittest
    p=SqlParser()
    p.build(debug=False)
    
    class UnitTestParset(unittest.TestCase):
        def test_show_tables(self):
            result = p.text("show tables;")
            print(result) 
            #self.assertEqual(result, ['show-tables'])

        def test_create_tables(self):
            result = p.text("create table ATABLE (ACOMLUMN INT PRIMARY KEY);", debug=False)
            print(result)
            #self.assertEqual(result,[('create-table', 'atable', ('acomlumn', 'int', ['primary-key']))])

        def test_delete(self):
            result = p.text("DELETE FROM table_name WHERE column_name = 5;", debug=False)
            print(result)
            #self.assertEqual(result,[('delete', 'table_name', ('=', 'column_name', 5))])
            
        def test_insert(self):
            result = p.text("INSERT INTO test VALUES (3, 45.34);", debug=True)
            print(result)
            #self.assertEqual(result,[('delete', 'table_name', ('=', 'column_name', 5))])

    ##unittest.main()
