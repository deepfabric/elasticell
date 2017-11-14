// Generated from /home/zhichyu/src/github.com/deepfabric/indexer/cql/parser/CQL.g4 by ANTLR 4.7.

package parser // CQL

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = reflect.Copy
var _ = strconv.Itoa

var parserATN = []uint16{
	3, 24715, 42794, 33075, 47597, 16764, 15335, 30598, 22884, 3, 35, 181,
	4, 2, 9, 2, 4, 3, 9, 3, 4, 4, 9, 4, 4, 5, 9, 5, 4, 6, 9, 6, 4, 7, 9, 7,
	4, 8, 9, 8, 4, 9, 9, 9, 4, 10, 9, 10, 4, 11, 9, 11, 4, 12, 9, 12, 4, 13,
	9, 13, 4, 14, 9, 14, 4, 15, 9, 15, 4, 16, 9, 16, 4, 17, 9, 17, 4, 18, 9,
	18, 4, 19, 9, 19, 4, 20, 9, 20, 4, 21, 9, 21, 4, 22, 9, 22, 4, 23, 9, 23,
	4, 24, 9, 24, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3,
	2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 5, 2, 64, 10, 2, 3, 3, 3, 3, 3, 3, 3,
	3, 7, 3, 70, 10, 3, 12, 3, 14, 3, 73, 11, 3, 3, 3, 7, 3, 76, 10, 3, 12,
	3, 14, 3, 79, 11, 3, 3, 3, 7, 3, 82, 10, 3, 12, 3, 14, 3, 85, 11, 3, 3,
	4, 3, 4, 3, 4, 3, 5, 3, 5, 3, 5, 3, 6, 3, 6, 3, 6, 3, 7, 3, 7, 3, 7, 3,
	7, 7, 7, 100, 10, 7, 12, 7, 14, 7, 103, 11, 7, 3, 7, 7, 7, 106, 10, 7,
	12, 7, 14, 7, 109, 11, 7, 3, 7, 7, 7, 112, 10, 7, 12, 7, 14, 7, 115, 11,
	7, 3, 7, 5, 7, 118, 10, 7, 3, 8, 3, 8, 3, 9, 3, 9, 3, 9, 6, 9, 125, 10,
	9, 13, 9, 14, 9, 126, 3, 10, 3, 10, 3, 10, 3, 11, 3, 11, 3, 11, 3, 12,
	3, 12, 3, 12, 3, 13, 3, 13, 3, 13, 3, 13, 5, 13, 142, 10, 13, 3, 14, 3,
	14, 3, 15, 3, 15, 3, 16, 3, 16, 3, 17, 3, 17, 3, 18, 3, 18, 3, 19, 3, 19,
	3, 19, 3, 19, 3, 20, 3, 20, 3, 20, 3, 20, 3, 21, 3, 21, 3, 21, 3, 21, 3,
	22, 3, 22, 3, 23, 3, 23, 3, 23, 3, 23, 7, 23, 172, 10, 23, 12, 23, 14,
	23, 175, 11, 23, 3, 23, 3, 23, 3, 24, 3, 24, 3, 24, 2, 2, 25, 2, 4, 6,
	8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42,
	44, 46, 2, 6, 3, 2, 8, 9, 3, 2, 16, 21, 3, 2, 31, 33, 3, 2, 26, 30, 2,
	171, 2, 63, 3, 2, 2, 2, 4, 65, 3, 2, 2, 2, 6, 86, 3, 2, 2, 2, 8, 89, 3,
	2, 2, 2, 10, 92, 3, 2, 2, 2, 12, 95, 3, 2, 2, 2, 14, 119, 3, 2, 2, 2, 16,
	121, 3, 2, 2, 2, 18, 128, 3, 2, 2, 2, 20, 131, 3, 2, 2, 2, 22, 134, 3,
	2, 2, 2, 24, 137, 3, 2, 2, 2, 26, 143, 3, 2, 2, 2, 28, 145, 3, 2, 2, 2,
	30, 147, 3, 2, 2, 2, 32, 149, 3, 2, 2, 2, 34, 151, 3, 2, 2, 2, 36, 153,
	3, 2, 2, 2, 38, 157, 3, 2, 2, 2, 40, 161, 3, 2, 2, 2, 42, 165, 3, 2, 2,
	2, 44, 167, 3, 2, 2, 2, 46, 178, 3, 2, 2, 2, 48, 49, 5, 4, 3, 2, 49, 50,
	7, 2, 2, 3, 50, 64, 3, 2, 2, 2, 51, 52, 5, 6, 4, 2, 52, 53, 7, 2, 2, 3,
	53, 64, 3, 2, 2, 2, 54, 55, 5, 8, 5, 2, 55, 56, 7, 2, 2, 3, 56, 64, 3,
	2, 2, 2, 57, 58, 5, 10, 6, 2, 58, 59, 7, 2, 2, 3, 59, 64, 3, 2, 2, 2, 60,
	61, 5, 12, 7, 2, 61, 62, 7, 2, 2, 3, 62, 64, 3, 2, 2, 2, 63, 48, 3, 2,
	2, 2, 63, 51, 3, 2, 2, 2, 63, 54, 3, 2, 2, 2, 63, 57, 3, 2, 2, 2, 63, 60,
	3, 2, 2, 2, 64, 3, 3, 2, 2, 2, 65, 66, 7, 3, 2, 2, 66, 67, 5, 14, 8, 2,
	67, 71, 7, 4, 2, 2, 68, 70, 5, 18, 10, 2, 69, 68, 3, 2, 2, 2, 70, 73, 3,
	2, 2, 2, 71, 69, 3, 2, 2, 2, 71, 72, 3, 2, 2, 2, 72, 77, 3, 2, 2, 2, 73,
	71, 3, 2, 2, 2, 74, 76, 5, 20, 11, 2, 75, 74, 3, 2, 2, 2, 76, 79, 3, 2,
	2, 2, 77, 75, 3, 2, 2, 2, 77, 78, 3, 2, 2, 2, 78, 83, 3, 2, 2, 2, 79, 77,
	3, 2, 2, 2, 80, 82, 5, 22, 12, 2, 81, 80, 3, 2, 2, 2, 82, 85, 3, 2, 2,
	2, 83, 81, 3, 2, 2, 2, 83, 84, 3, 2, 2, 2, 84, 5, 3, 2, 2, 2, 85, 83, 3,
	2, 2, 2, 86, 87, 7, 5, 2, 2, 87, 88, 5, 14, 8, 2, 88, 7, 3, 2, 2, 2, 89,
	90, 7, 6, 2, 2, 90, 91, 5, 16, 9, 2, 91, 9, 3, 2, 2, 2, 92, 93, 7, 7, 2,
	2, 93, 94, 5, 16, 9, 2, 94, 11, 3, 2, 2, 2, 95, 96, 9, 2, 2, 2, 96, 97,
	5, 14, 8, 2, 97, 101, 7, 10, 2, 2, 98, 100, 5, 36, 19, 2, 99, 98, 3, 2,
	2, 2, 100, 103, 3, 2, 2, 2, 101, 99, 3, 2, 2, 2, 101, 102, 3, 2, 2, 2,
	102, 107, 3, 2, 2, 2, 103, 101, 3, 2, 2, 2, 104, 106, 5, 38, 20, 2, 105,
	104, 3, 2, 2, 2, 106, 109, 3, 2, 2, 2, 107, 105, 3, 2, 2, 2, 107, 108,
	3, 2, 2, 2, 108, 113, 3, 2, 2, 2, 109, 107, 3, 2, 2, 2, 110, 112, 5, 40,
	21, 2, 111, 110, 3, 2, 2, 2, 112, 115, 3, 2, 2, 2, 113, 111, 3, 2, 2, 2,
	113, 114, 3, 2, 2, 2, 114, 117, 3, 2, 2, 2, 115, 113, 3, 2, 2, 2, 116,
	118, 5, 24, 13, 2, 117, 116, 3, 2, 2, 2, 117, 118, 3, 2, 2, 2, 118, 13,
	3, 2, 2, 2, 119, 120, 7, 34, 2, 2, 120, 15, 3, 2, 2, 2, 121, 122, 5, 14,
	8, 2, 122, 124, 5, 32, 17, 2, 123, 125, 5, 34, 18, 2, 124, 123, 3, 2, 2,
	2, 125, 126, 3, 2, 2, 2, 126, 124, 3, 2, 2, 2, 126, 127, 3, 2, 2, 2, 127,
	17, 3, 2, 2, 2, 128, 129, 5, 28, 15, 2, 129, 130, 5, 30, 16, 2, 130, 19,
	3, 2, 2, 2, 131, 132, 5, 28, 15, 2, 132, 133, 7, 22, 2, 2, 133, 21, 3,
	2, 2, 2, 134, 135, 5, 28, 15, 2, 135, 136, 7, 23, 2, 2, 136, 23, 3, 2,
	2, 2, 137, 138, 7, 11, 2, 2, 138, 141, 5, 26, 14, 2, 139, 140, 7, 12, 2,
	2, 140, 142, 5, 46, 24, 2, 141, 139, 3, 2, 2, 2, 141, 142, 3, 2, 2, 2,
	142, 25, 3, 2, 2, 2, 143, 144, 5, 28, 15, 2, 144, 27, 3, 2, 2, 2, 145,
	146, 7, 34, 2, 2, 146, 29, 3, 2, 2, 2, 147, 148, 9, 3, 2, 2, 148, 31, 3,
	2, 2, 2, 149, 150, 7, 33, 2, 2, 150, 33, 3, 2, 2, 2, 151, 152, 9, 4, 2,
	2, 152, 35, 3, 2, 2, 2, 153, 154, 5, 28, 15, 2, 154, 155, 5, 42, 22, 2,
	155, 156, 5, 34, 18, 2, 156, 37, 3, 2, 2, 2, 157, 158, 5, 28, 15, 2, 158,
	159, 7, 24, 2, 2, 159, 160, 5, 44, 23, 2, 160, 39, 3, 2, 2, 2, 161, 162,
	5, 28, 15, 2, 162, 163, 7, 25, 2, 2, 163, 164, 7, 32, 2, 2, 164, 41, 3,
	2, 2, 2, 165, 166, 9, 5, 2, 2, 166, 43, 3, 2, 2, 2, 167, 168, 7, 13, 2,
	2, 168, 173, 7, 33, 2, 2, 169, 170, 7, 14, 2, 2, 170, 172, 7, 33, 2, 2,
	171, 169, 3, 2, 2, 2, 172, 175, 3, 2, 2, 2, 173, 171, 3, 2, 2, 2, 173,
	174, 3, 2, 2, 2, 174, 176, 3, 2, 2, 2, 175, 173, 3, 2, 2, 2, 176, 177,
	7, 15, 2, 2, 177, 45, 3, 2, 2, 2, 178, 179, 7, 33, 2, 2, 179, 47, 3, 2,
	2, 2, 13, 63, 71, 77, 83, 101, 107, 113, 117, 126, 141, 173,
}
var deserializer = antlr.NewATNDeserializer(nil)
var deserializedATN = deserializer.DeserializeFromUInt16(parserATN)

var literalNames = []string{
	"", "'IDX.CREATE'", "'SCHEMA'", "'IDX.DESTROY'", "'IDX.INSERT'", "'IDX.DEL'",
	"'IDX.SELECT'", "'QUERY'", "'WHERE'", "'ORDERBY'", "'LIMIT'", "'['", "','",
	"']'", "'UINT8'", "'UINT16'", "'UINT32'", "'UINT64'", "'FLOAT32'", "'FLOAT64'",
	"'ENUM'", "'STRING'", "'IN'", "'CONTAINS'", "'<'", "'>'", "'='", "'<='",
	"'>='",
}
var symbolicNames = []string{
	"", "", "", "", "", "", "", "", "", "", "", "", "", "", "K_UINT8", "K_UINT16",
	"K_UINT32", "K_UINT64", "K_FLOAT32", "K_FLOAT64", "K_ENUM", "K_STRING",
	"K_IN", "K_CONTAINS", "K_LT", "K_BT", "K_EQ", "K_LE", "K_BE", "FLOAT_LIT",
	"STRING", "INT", "IDENTIFIER", "WS",
}

var ruleNames = []string{
	"cql", "create", "destroy", "insert", "del", "query", "indexName", "document",
	"uintPropDef", "enumPropDef", "strPropDef", "orderLimit", "order", "property",
	"uintType", "docId", "value", "uintPred", "enumPred", "strPred", "compare",
	"intList", "limit",
}
var decisionToDFA = make([]*antlr.DFA, len(deserializedATN.DecisionToState))

func init() {
	for index, ds := range deserializedATN.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(ds, index)
	}
}

type CQLParser struct {
	*antlr.BaseParser
}

func NewCQLParser(input antlr.TokenStream) *CQLParser {
	this := new(CQLParser)

	this.BaseParser = antlr.NewBaseParser(input)

	this.Interpreter = antlr.NewParserATNSimulator(this, deserializedATN, decisionToDFA, antlr.NewPredictionContextCache())
	this.RuleNames = ruleNames
	this.LiteralNames = literalNames
	this.SymbolicNames = symbolicNames
	this.GrammarFileName = "CQL.g4"

	return this
}

// CQLParser tokens.
const (
	CQLParserEOF        = antlr.TokenEOF
	CQLParserT__0       = 1
	CQLParserT__1       = 2
	CQLParserT__2       = 3
	CQLParserT__3       = 4
	CQLParserT__4       = 5
	CQLParserT__5       = 6
	CQLParserT__6       = 7
	CQLParserT__7       = 8
	CQLParserT__8       = 9
	CQLParserT__9       = 10
	CQLParserT__10      = 11
	CQLParserT__11      = 12
	CQLParserT__12      = 13
	CQLParserK_UINT8    = 14
	CQLParserK_UINT16   = 15
	CQLParserK_UINT32   = 16
	CQLParserK_UINT64   = 17
	CQLParserK_FLOAT32  = 18
	CQLParserK_FLOAT64  = 19
	CQLParserK_ENUM     = 20
	CQLParserK_STRING   = 21
	CQLParserK_IN       = 22
	CQLParserK_CONTAINS = 23
	CQLParserK_LT       = 24
	CQLParserK_BT       = 25
	CQLParserK_EQ       = 26
	CQLParserK_LE       = 27
	CQLParserK_BE       = 28
	CQLParserFLOAT_LIT  = 29
	CQLParserSTRING     = 30
	CQLParserINT        = 31
	CQLParserIDENTIFIER = 32
	CQLParserWS         = 33
)

// CQLParser rules.
const (
	CQLParserRULE_cql         = 0
	CQLParserRULE_create      = 1
	CQLParserRULE_destroy     = 2
	CQLParserRULE_insert      = 3
	CQLParserRULE_del         = 4
	CQLParserRULE_query       = 5
	CQLParserRULE_indexName   = 6
	CQLParserRULE_document    = 7
	CQLParserRULE_uintPropDef = 8
	CQLParserRULE_enumPropDef = 9
	CQLParserRULE_strPropDef  = 10
	CQLParserRULE_orderLimit  = 11
	CQLParserRULE_order       = 12
	CQLParserRULE_property    = 13
	CQLParserRULE_uintType    = 14
	CQLParserRULE_docId       = 15
	CQLParserRULE_value       = 16
	CQLParserRULE_uintPred    = 17
	CQLParserRULE_enumPred    = 18
	CQLParserRULE_strPred     = 19
	CQLParserRULE_compare     = 20
	CQLParserRULE_intList     = 21
	CQLParserRULE_limit       = 22
)

// ICqlContext is an interface to support dynamic dispatch.
type ICqlContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCqlContext differentiates from other interfaces.
	IsCqlContext()
}

type CqlContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCqlContext() *CqlContext {
	var p = new(CqlContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_cql
	return p
}

func (*CqlContext) IsCqlContext() {}

func NewCqlContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CqlContext {
	var p = new(CqlContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_cql

	return p
}

func (s *CqlContext) GetParser() antlr.Parser { return s.parser }

func (s *CqlContext) Create() ICreateContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICreateContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ICreateContext)
}

func (s *CqlContext) EOF() antlr.TerminalNode {
	return s.GetToken(CQLParserEOF, 0)
}

func (s *CqlContext) Destroy() IDestroyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDestroyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDestroyContext)
}

func (s *CqlContext) Insert() IInsertContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IInsertContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IInsertContext)
}

func (s *CqlContext) Del() IDelContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDelContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDelContext)
}

func (s *CqlContext) Query() IQueryContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IQueryContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IQueryContext)
}

func (s *CqlContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CqlContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CqlContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterCql(s)
	}
}

func (s *CqlContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitCql(s)
	}
}

func (s *CqlContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitCql(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Cql() (localctx ICqlContext) {
	localctx = NewCqlContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, CQLParserRULE_cql)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(61)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case CQLParserT__0:
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(46)
			p.Create()
		}
		{
			p.SetState(47)
			p.Match(CQLParserEOF)
		}

	case CQLParserT__2:
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(49)
			p.Destroy()
		}
		{
			p.SetState(50)
			p.Match(CQLParserEOF)
		}

	case CQLParserT__3:
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(52)
			p.Insert()
		}
		{
			p.SetState(53)
			p.Match(CQLParserEOF)
		}

	case CQLParserT__4:
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(55)
			p.Del()
		}
		{
			p.SetState(56)
			p.Match(CQLParserEOF)
		}

	case CQLParserT__5, CQLParserT__6:
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(58)
			p.Query()
		}
		{
			p.SetState(59)
			p.Match(CQLParserEOF)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// ICreateContext is an interface to support dynamic dispatch.
type ICreateContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCreateContext differentiates from other interfaces.
	IsCreateContext()
}

type CreateContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCreateContext() *CreateContext {
	var p = new(CreateContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_create
	return p
}

func (*CreateContext) IsCreateContext() {}

func NewCreateContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CreateContext {
	var p = new(CreateContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_create

	return p
}

func (s *CreateContext) GetParser() antlr.Parser { return s.parser }

func (s *CreateContext) IndexName() IIndexNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIndexNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIndexNameContext)
}

func (s *CreateContext) AllUintPropDef() []IUintPropDefContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IUintPropDefContext)(nil)).Elem())
	var tst = make([]IUintPropDefContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IUintPropDefContext)
		}
	}

	return tst
}

func (s *CreateContext) UintPropDef(i int) IUintPropDefContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUintPropDefContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IUintPropDefContext)
}

func (s *CreateContext) AllEnumPropDef() []IEnumPropDefContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IEnumPropDefContext)(nil)).Elem())
	var tst = make([]IEnumPropDefContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IEnumPropDefContext)
		}
	}

	return tst
}

func (s *CreateContext) EnumPropDef(i int) IEnumPropDefContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IEnumPropDefContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IEnumPropDefContext)
}

func (s *CreateContext) AllStrPropDef() []IStrPropDefContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IStrPropDefContext)(nil)).Elem())
	var tst = make([]IStrPropDefContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IStrPropDefContext)
		}
	}

	return tst
}

func (s *CreateContext) StrPropDef(i int) IStrPropDefContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IStrPropDefContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IStrPropDefContext)
}

func (s *CreateContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CreateContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CreateContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterCreate(s)
	}
}

func (s *CreateContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitCreate(s)
	}
}

func (s *CreateContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitCreate(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Create() (localctx ICreateContext) {
	localctx = NewCreateContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, CQLParserRULE_create)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(63)
		p.Match(CQLParserT__0)
	}
	{
		p.SetState(64)
		p.IndexName()
	}
	{
		p.SetState(65)
		p.Match(CQLParserT__1)
	}
	p.SetState(69)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 1, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(66)
				p.UintPropDef()
			}

		}
		p.SetState(71)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 1, p.GetParserRuleContext())
	}
	p.SetState(75)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 2, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(72)
				p.EnumPropDef()
			}

		}
		p.SetState(77)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 2, p.GetParserRuleContext())
	}
	p.SetState(81)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == CQLParserIDENTIFIER {
		{
			p.SetState(78)
			p.StrPropDef()
		}

		p.SetState(83)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}

	return localctx
}

// IDestroyContext is an interface to support dynamic dispatch.
type IDestroyContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsDestroyContext differentiates from other interfaces.
	IsDestroyContext()
}

type DestroyContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDestroyContext() *DestroyContext {
	var p = new(DestroyContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_destroy
	return p
}

func (*DestroyContext) IsDestroyContext() {}

func NewDestroyContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DestroyContext {
	var p = new(DestroyContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_destroy

	return p
}

func (s *DestroyContext) GetParser() antlr.Parser { return s.parser }

func (s *DestroyContext) IndexName() IIndexNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIndexNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIndexNameContext)
}

func (s *DestroyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestroyContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DestroyContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterDestroy(s)
	}
}

func (s *DestroyContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitDestroy(s)
	}
}

func (s *DestroyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitDestroy(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Destroy() (localctx IDestroyContext) {
	localctx = NewDestroyContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, CQLParserRULE_destroy)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(84)
		p.Match(CQLParserT__2)
	}
	{
		p.SetState(85)
		p.IndexName()
	}

	return localctx
}

// IInsertContext is an interface to support dynamic dispatch.
type IInsertContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsInsertContext differentiates from other interfaces.
	IsInsertContext()
}

type InsertContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyInsertContext() *InsertContext {
	var p = new(InsertContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_insert
	return p
}

func (*InsertContext) IsInsertContext() {}

func NewInsertContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *InsertContext {
	var p = new(InsertContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_insert

	return p
}

func (s *InsertContext) GetParser() antlr.Parser { return s.parser }

func (s *InsertContext) Document() IDocumentContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDocumentContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDocumentContext)
}

func (s *InsertContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *InsertContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *InsertContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterInsert(s)
	}
}

func (s *InsertContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitInsert(s)
	}
}

func (s *InsertContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitInsert(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Insert() (localctx IInsertContext) {
	localctx = NewInsertContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, CQLParserRULE_insert)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(87)
		p.Match(CQLParserT__3)
	}
	{
		p.SetState(88)
		p.Document()
	}

	return localctx
}

// IDelContext is an interface to support dynamic dispatch.
type IDelContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsDelContext differentiates from other interfaces.
	IsDelContext()
}

type DelContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDelContext() *DelContext {
	var p = new(DelContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_del
	return p
}

func (*DelContext) IsDelContext() {}

func NewDelContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DelContext {
	var p = new(DelContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_del

	return p
}

func (s *DelContext) GetParser() antlr.Parser { return s.parser }

func (s *DelContext) Document() IDocumentContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDocumentContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDocumentContext)
}

func (s *DelContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DelContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DelContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterDel(s)
	}
}

func (s *DelContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitDel(s)
	}
}

func (s *DelContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitDel(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Del() (localctx IDelContext) {
	localctx = NewDelContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, CQLParserRULE_del)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(90)
		p.Match(CQLParserT__4)
	}
	{
		p.SetState(91)
		p.Document()
	}

	return localctx
}

// IQueryContext is an interface to support dynamic dispatch.
type IQueryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsQueryContext differentiates from other interfaces.
	IsQueryContext()
}

type QueryContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyQueryContext() *QueryContext {
	var p = new(QueryContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_query
	return p
}

func (*QueryContext) IsQueryContext() {}

func NewQueryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *QueryContext {
	var p = new(QueryContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_query

	return p
}

func (s *QueryContext) GetParser() antlr.Parser { return s.parser }

func (s *QueryContext) IndexName() IIndexNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIndexNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIndexNameContext)
}

func (s *QueryContext) AllUintPred() []IUintPredContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IUintPredContext)(nil)).Elem())
	var tst = make([]IUintPredContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IUintPredContext)
		}
	}

	return tst
}

func (s *QueryContext) UintPred(i int) IUintPredContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUintPredContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IUintPredContext)
}

func (s *QueryContext) AllEnumPred() []IEnumPredContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IEnumPredContext)(nil)).Elem())
	var tst = make([]IEnumPredContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IEnumPredContext)
		}
	}

	return tst
}

func (s *QueryContext) EnumPred(i int) IEnumPredContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IEnumPredContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IEnumPredContext)
}

func (s *QueryContext) AllStrPred() []IStrPredContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IStrPredContext)(nil)).Elem())
	var tst = make([]IStrPredContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IStrPredContext)
		}
	}

	return tst
}

func (s *QueryContext) StrPred(i int) IStrPredContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IStrPredContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IStrPredContext)
}

func (s *QueryContext) OrderLimit() IOrderLimitContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IOrderLimitContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IOrderLimitContext)
}

func (s *QueryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *QueryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *QueryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterQuery(s)
	}
}

func (s *QueryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitQuery(s)
	}
}

func (s *QueryContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitQuery(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Query() (localctx IQueryContext) {
	localctx = NewQueryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, CQLParserRULE_query)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(93)
	_la = p.GetTokenStream().LA(1)

	if !(_la == CQLParserT__5 || _la == CQLParserT__6) {
		p.GetErrorHandler().RecoverInline(p)
	} else {
		p.GetErrorHandler().ReportMatch(p)
		p.Consume()
	}
	{
		p.SetState(94)
		p.IndexName()
	}
	{
		p.SetState(95)
		p.Match(CQLParserT__7)
	}
	p.SetState(99)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 4, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(96)
				p.UintPred()
			}

		}
		p.SetState(101)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 4, p.GetParserRuleContext())
	}
	p.SetState(105)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 5, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(102)
				p.EnumPred()
			}

		}
		p.SetState(107)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 5, p.GetParserRuleContext())
	}
	p.SetState(111)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == CQLParserIDENTIFIER {
		{
			p.SetState(108)
			p.StrPred()
		}

		p.SetState(113)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	p.SetState(115)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == CQLParserT__8 {
		{
			p.SetState(114)
			p.OrderLimit()
		}

	}

	return localctx
}

// IIndexNameContext is an interface to support dynamic dispatch.
type IIndexNameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsIndexNameContext differentiates from other interfaces.
	IsIndexNameContext()
}

type IndexNameContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyIndexNameContext() *IndexNameContext {
	var p = new(IndexNameContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_indexName
	return p
}

func (*IndexNameContext) IsIndexNameContext() {}

func NewIndexNameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *IndexNameContext {
	var p = new(IndexNameContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_indexName

	return p
}

func (s *IndexNameContext) GetParser() antlr.Parser { return s.parser }

func (s *IndexNameContext) IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(CQLParserIDENTIFIER, 0)
}

func (s *IndexNameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IndexNameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *IndexNameContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterIndexName(s)
	}
}

func (s *IndexNameContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitIndexName(s)
	}
}

func (s *IndexNameContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitIndexName(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) IndexName() (localctx IIndexNameContext) {
	localctx = NewIndexNameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, CQLParserRULE_indexName)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(117)
		p.Match(CQLParserIDENTIFIER)
	}

	return localctx
}

// IDocumentContext is an interface to support dynamic dispatch.
type IDocumentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsDocumentContext differentiates from other interfaces.
	IsDocumentContext()
}

type DocumentContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDocumentContext() *DocumentContext {
	var p = new(DocumentContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_document
	return p
}

func (*DocumentContext) IsDocumentContext() {}

func NewDocumentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DocumentContext {
	var p = new(DocumentContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_document

	return p
}

func (s *DocumentContext) GetParser() antlr.Parser { return s.parser }

func (s *DocumentContext) IndexName() IIndexNameContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIndexNameContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIndexNameContext)
}

func (s *DocumentContext) DocId() IDocIdContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IDocIdContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IDocIdContext)
}

func (s *DocumentContext) AllValue() []IValueContext {
	var ts = s.GetTypedRuleContexts(reflect.TypeOf((*IValueContext)(nil)).Elem())
	var tst = make([]IValueContext, len(ts))

	for i, t := range ts {
		if t != nil {
			tst[i] = t.(IValueContext)
		}
	}

	return tst
}

func (s *DocumentContext) Value(i int) IValueContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IValueContext)(nil)).Elem(), i)

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *DocumentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DocumentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DocumentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterDocument(s)
	}
}

func (s *DocumentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitDocument(s)
	}
}

func (s *DocumentContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitDocument(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Document() (localctx IDocumentContext) {
	localctx = NewDocumentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, CQLParserRULE_document)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(119)
		p.IndexName()
	}
	{
		p.SetState(120)
		p.DocId()
	}
	p.SetState(122)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = (((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<CQLParserFLOAT_LIT)|(1<<CQLParserSTRING)|(1<<CQLParserINT))) != 0) {
		{
			p.SetState(121)
			p.Value()
		}

		p.SetState(124)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}

	return localctx
}

// IUintPropDefContext is an interface to support dynamic dispatch.
type IUintPropDefContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsUintPropDefContext differentiates from other interfaces.
	IsUintPropDefContext()
}

type UintPropDefContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUintPropDefContext() *UintPropDefContext {
	var p = new(UintPropDefContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_uintPropDef
	return p
}

func (*UintPropDefContext) IsUintPropDefContext() {}

func NewUintPropDefContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UintPropDefContext {
	var p = new(UintPropDefContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_uintPropDef

	return p
}

func (s *UintPropDefContext) GetParser() antlr.Parser { return s.parser }

func (s *UintPropDefContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *UintPropDefContext) UintType() IUintTypeContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IUintTypeContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IUintTypeContext)
}

func (s *UintPropDefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UintPropDefContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UintPropDefContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterUintPropDef(s)
	}
}

func (s *UintPropDefContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitUintPropDef(s)
	}
}

func (s *UintPropDefContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitUintPropDef(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) UintPropDef() (localctx IUintPropDefContext) {
	localctx = NewUintPropDefContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, CQLParserRULE_uintPropDef)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(126)
		p.Property()
	}
	{
		p.SetState(127)
		p.UintType()
	}

	return localctx
}

// IEnumPropDefContext is an interface to support dynamic dispatch.
type IEnumPropDefContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsEnumPropDefContext differentiates from other interfaces.
	IsEnumPropDefContext()
}

type EnumPropDefContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyEnumPropDefContext() *EnumPropDefContext {
	var p = new(EnumPropDefContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_enumPropDef
	return p
}

func (*EnumPropDefContext) IsEnumPropDefContext() {}

func NewEnumPropDefContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *EnumPropDefContext {
	var p = new(EnumPropDefContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_enumPropDef

	return p
}

func (s *EnumPropDefContext) GetParser() antlr.Parser { return s.parser }

func (s *EnumPropDefContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *EnumPropDefContext) K_ENUM() antlr.TerminalNode {
	return s.GetToken(CQLParserK_ENUM, 0)
}

func (s *EnumPropDefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EnumPropDefContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *EnumPropDefContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterEnumPropDef(s)
	}
}

func (s *EnumPropDefContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitEnumPropDef(s)
	}
}

func (s *EnumPropDefContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitEnumPropDef(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) EnumPropDef() (localctx IEnumPropDefContext) {
	localctx = NewEnumPropDefContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, CQLParserRULE_enumPropDef)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(129)
		p.Property()
	}
	{
		p.SetState(130)
		p.Match(CQLParserK_ENUM)
	}

	return localctx
}

// IStrPropDefContext is an interface to support dynamic dispatch.
type IStrPropDefContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsStrPropDefContext differentiates from other interfaces.
	IsStrPropDefContext()
}

type StrPropDefContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyStrPropDefContext() *StrPropDefContext {
	var p = new(StrPropDefContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_strPropDef
	return p
}

func (*StrPropDefContext) IsStrPropDefContext() {}

func NewStrPropDefContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *StrPropDefContext {
	var p = new(StrPropDefContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_strPropDef

	return p
}

func (s *StrPropDefContext) GetParser() antlr.Parser { return s.parser }

func (s *StrPropDefContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *StrPropDefContext) K_STRING() antlr.TerminalNode {
	return s.GetToken(CQLParserK_STRING, 0)
}

func (s *StrPropDefContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StrPropDefContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *StrPropDefContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterStrPropDef(s)
	}
}

func (s *StrPropDefContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitStrPropDef(s)
	}
}

func (s *StrPropDefContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitStrPropDef(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) StrPropDef() (localctx IStrPropDefContext) {
	localctx = NewStrPropDefContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 20, CQLParserRULE_strPropDef)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(132)
		p.Property()
	}
	{
		p.SetState(133)
		p.Match(CQLParserK_STRING)
	}

	return localctx
}

// IOrderLimitContext is an interface to support dynamic dispatch.
type IOrderLimitContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsOrderLimitContext differentiates from other interfaces.
	IsOrderLimitContext()
}

type OrderLimitContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyOrderLimitContext() *OrderLimitContext {
	var p = new(OrderLimitContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_orderLimit
	return p
}

func (*OrderLimitContext) IsOrderLimitContext() {}

func NewOrderLimitContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *OrderLimitContext {
	var p = new(OrderLimitContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_orderLimit

	return p
}

func (s *OrderLimitContext) GetParser() antlr.Parser { return s.parser }

func (s *OrderLimitContext) Order() IOrderContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IOrderContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IOrderContext)
}

func (s *OrderLimitContext) Limit() ILimitContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ILimitContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ILimitContext)
}

func (s *OrderLimitContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OrderLimitContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *OrderLimitContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterOrderLimit(s)
	}
}

func (s *OrderLimitContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitOrderLimit(s)
	}
}

func (s *OrderLimitContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitOrderLimit(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) OrderLimit() (localctx IOrderLimitContext) {
	localctx = NewOrderLimitContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, CQLParserRULE_orderLimit)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(135)
		p.Match(CQLParserT__8)
	}
	{
		p.SetState(136)
		p.Order()
	}
	p.SetState(139)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == CQLParserT__9 {
		{
			p.SetState(137)
			p.Match(CQLParserT__9)
		}
		{
			p.SetState(138)
			p.Limit()
		}

	}

	return localctx
}

// IOrderContext is an interface to support dynamic dispatch.
type IOrderContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsOrderContext differentiates from other interfaces.
	IsOrderContext()
}

type OrderContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyOrderContext() *OrderContext {
	var p = new(OrderContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_order
	return p
}

func (*OrderContext) IsOrderContext() {}

func NewOrderContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *OrderContext {
	var p = new(OrderContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_order

	return p
}

func (s *OrderContext) GetParser() antlr.Parser { return s.parser }

func (s *OrderContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *OrderContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OrderContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *OrderContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterOrder(s)
	}
}

func (s *OrderContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitOrder(s)
	}
}

func (s *OrderContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitOrder(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Order() (localctx IOrderContext) {
	localctx = NewOrderContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, CQLParserRULE_order)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(141)
		p.Property()
	}

	return localctx
}

// IPropertyContext is an interface to support dynamic dispatch.
type IPropertyContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsPropertyContext differentiates from other interfaces.
	IsPropertyContext()
}

type PropertyContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyPropertyContext() *PropertyContext {
	var p = new(PropertyContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_property
	return p
}

func (*PropertyContext) IsPropertyContext() {}

func NewPropertyContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *PropertyContext {
	var p = new(PropertyContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_property

	return p
}

func (s *PropertyContext) GetParser() antlr.Parser { return s.parser }

func (s *PropertyContext) IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(CQLParserIDENTIFIER, 0)
}

func (s *PropertyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PropertyContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *PropertyContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterProperty(s)
	}
}

func (s *PropertyContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitProperty(s)
	}
}

func (s *PropertyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitProperty(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Property() (localctx IPropertyContext) {
	localctx = NewPropertyContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, CQLParserRULE_property)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(143)
		p.Match(CQLParserIDENTIFIER)
	}

	return localctx
}

// IUintTypeContext is an interface to support dynamic dispatch.
type IUintTypeContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsUintTypeContext differentiates from other interfaces.
	IsUintTypeContext()
}

type UintTypeContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUintTypeContext() *UintTypeContext {
	var p = new(UintTypeContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_uintType
	return p
}

func (*UintTypeContext) IsUintTypeContext() {}

func NewUintTypeContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UintTypeContext {
	var p = new(UintTypeContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_uintType

	return p
}

func (s *UintTypeContext) GetParser() antlr.Parser { return s.parser }

func (s *UintTypeContext) K_UINT8() antlr.TerminalNode {
	return s.GetToken(CQLParserK_UINT8, 0)
}

func (s *UintTypeContext) K_UINT16() antlr.TerminalNode {
	return s.GetToken(CQLParserK_UINT16, 0)
}

func (s *UintTypeContext) K_UINT32() antlr.TerminalNode {
	return s.GetToken(CQLParserK_UINT32, 0)
}

func (s *UintTypeContext) K_UINT64() antlr.TerminalNode {
	return s.GetToken(CQLParserK_UINT64, 0)
}

func (s *UintTypeContext) K_FLOAT32() antlr.TerminalNode {
	return s.GetToken(CQLParserK_FLOAT32, 0)
}

func (s *UintTypeContext) K_FLOAT64() antlr.TerminalNode {
	return s.GetToken(CQLParserK_FLOAT64, 0)
}

func (s *UintTypeContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UintTypeContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UintTypeContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterUintType(s)
	}
}

func (s *UintTypeContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitUintType(s)
	}
}

func (s *UintTypeContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitUintType(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) UintType() (localctx IUintTypeContext) {
	localctx = NewUintTypeContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 28, CQLParserRULE_uintType)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(145)
	_la = p.GetTokenStream().LA(1)

	if !(((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<CQLParserK_UINT8)|(1<<CQLParserK_UINT16)|(1<<CQLParserK_UINT32)|(1<<CQLParserK_UINT64)|(1<<CQLParserK_FLOAT32)|(1<<CQLParserK_FLOAT64))) != 0) {
		p.GetErrorHandler().RecoverInline(p)
	} else {
		p.GetErrorHandler().ReportMatch(p)
		p.Consume()
	}

	return localctx
}

// IDocIdContext is an interface to support dynamic dispatch.
type IDocIdContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsDocIdContext differentiates from other interfaces.
	IsDocIdContext()
}

type DocIdContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDocIdContext() *DocIdContext {
	var p = new(DocIdContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_docId
	return p
}

func (*DocIdContext) IsDocIdContext() {}

func NewDocIdContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DocIdContext {
	var p = new(DocIdContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_docId

	return p
}

func (s *DocIdContext) GetParser() antlr.Parser { return s.parser }

func (s *DocIdContext) INT() antlr.TerminalNode {
	return s.GetToken(CQLParserINT, 0)
}

func (s *DocIdContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DocIdContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DocIdContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterDocId(s)
	}
}

func (s *DocIdContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitDocId(s)
	}
}

func (s *DocIdContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitDocId(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) DocId() (localctx IDocIdContext) {
	localctx = NewDocIdContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 30, CQLParserRULE_docId)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(147)
		p.Match(CQLParserINT)
	}

	return localctx
}

// IValueContext is an interface to support dynamic dispatch.
type IValueContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsValueContext differentiates from other interfaces.
	IsValueContext()
}

type ValueContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyValueContext() *ValueContext {
	var p = new(ValueContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_value
	return p
}

func (*ValueContext) IsValueContext() {}

func NewValueContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ValueContext {
	var p = new(ValueContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_value

	return p
}

func (s *ValueContext) GetParser() antlr.Parser { return s.parser }

func (s *ValueContext) INT() antlr.TerminalNode {
	return s.GetToken(CQLParserINT, 0)
}

func (s *ValueContext) FLOAT_LIT() antlr.TerminalNode {
	return s.GetToken(CQLParserFLOAT_LIT, 0)
}

func (s *ValueContext) STRING() antlr.TerminalNode {
	return s.GetToken(CQLParserSTRING, 0)
}

func (s *ValueContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ValueContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterValue(s)
	}
}

func (s *ValueContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitValue(s)
	}
}

func (s *ValueContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitValue(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Value() (localctx IValueContext) {
	localctx = NewValueContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 32, CQLParserRULE_value)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(149)
	_la = p.GetTokenStream().LA(1)

	if !(((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<CQLParserFLOAT_LIT)|(1<<CQLParserSTRING)|(1<<CQLParserINT))) != 0) {
		p.GetErrorHandler().RecoverInline(p)
	} else {
		p.GetErrorHandler().ReportMatch(p)
		p.Consume()
	}

	return localctx
}

// IUintPredContext is an interface to support dynamic dispatch.
type IUintPredContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsUintPredContext differentiates from other interfaces.
	IsUintPredContext()
}

type UintPredContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyUintPredContext() *UintPredContext {
	var p = new(UintPredContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_uintPred
	return p
}

func (*UintPredContext) IsUintPredContext() {}

func NewUintPredContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *UintPredContext {
	var p = new(UintPredContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_uintPred

	return p
}

func (s *UintPredContext) GetParser() antlr.Parser { return s.parser }

func (s *UintPredContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *UintPredContext) Compare() ICompareContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*ICompareContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(ICompareContext)
}

func (s *UintPredContext) Value() IValueContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IValueContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IValueContext)
}

func (s *UintPredContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UintPredContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *UintPredContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterUintPred(s)
	}
}

func (s *UintPredContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitUintPred(s)
	}
}

func (s *UintPredContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitUintPred(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) UintPred() (localctx IUintPredContext) {
	localctx = NewUintPredContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 34, CQLParserRULE_uintPred)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(151)
		p.Property()
	}
	{
		p.SetState(152)
		p.Compare()
	}
	{
		p.SetState(153)
		p.Value()
	}

	return localctx
}

// IEnumPredContext is an interface to support dynamic dispatch.
type IEnumPredContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsEnumPredContext differentiates from other interfaces.
	IsEnumPredContext()
}

type EnumPredContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyEnumPredContext() *EnumPredContext {
	var p = new(EnumPredContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_enumPred
	return p
}

func (*EnumPredContext) IsEnumPredContext() {}

func NewEnumPredContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *EnumPredContext {
	var p = new(EnumPredContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_enumPred

	return p
}

func (s *EnumPredContext) GetParser() antlr.Parser { return s.parser }

func (s *EnumPredContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *EnumPredContext) K_IN() antlr.TerminalNode {
	return s.GetToken(CQLParserK_IN, 0)
}

func (s *EnumPredContext) IntList() IIntListContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IIntListContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IIntListContext)
}

func (s *EnumPredContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *EnumPredContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *EnumPredContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterEnumPred(s)
	}
}

func (s *EnumPredContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitEnumPred(s)
	}
}

func (s *EnumPredContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitEnumPred(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) EnumPred() (localctx IEnumPredContext) {
	localctx = NewEnumPredContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 36, CQLParserRULE_enumPred)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(155)
		p.Property()
	}
	{
		p.SetState(156)
		p.Match(CQLParserK_IN)
	}
	{
		p.SetState(157)
		p.IntList()
	}

	return localctx
}

// IStrPredContext is an interface to support dynamic dispatch.
type IStrPredContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsStrPredContext differentiates from other interfaces.
	IsStrPredContext()
}

type StrPredContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyStrPredContext() *StrPredContext {
	var p = new(StrPredContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_strPred
	return p
}

func (*StrPredContext) IsStrPredContext() {}

func NewStrPredContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *StrPredContext {
	var p = new(StrPredContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_strPred

	return p
}

func (s *StrPredContext) GetParser() antlr.Parser { return s.parser }

func (s *StrPredContext) Property() IPropertyContext {
	var t = s.GetTypedRuleContext(reflect.TypeOf((*IPropertyContext)(nil)).Elem(), 0)

	if t == nil {
		return nil
	}

	return t.(IPropertyContext)
}

func (s *StrPredContext) K_CONTAINS() antlr.TerminalNode {
	return s.GetToken(CQLParserK_CONTAINS, 0)
}

func (s *StrPredContext) STRING() antlr.TerminalNode {
	return s.GetToken(CQLParserSTRING, 0)
}

func (s *StrPredContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StrPredContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *StrPredContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterStrPred(s)
	}
}

func (s *StrPredContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitStrPred(s)
	}
}

func (s *StrPredContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitStrPred(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) StrPred() (localctx IStrPredContext) {
	localctx = NewStrPredContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 38, CQLParserRULE_strPred)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(159)
		p.Property()
	}
	{
		p.SetState(160)
		p.Match(CQLParserK_CONTAINS)
	}
	{
		p.SetState(161)
		p.Match(CQLParserSTRING)
	}

	return localctx
}

// ICompareContext is an interface to support dynamic dispatch.
type ICompareContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsCompareContext differentiates from other interfaces.
	IsCompareContext()
}

type CompareContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyCompareContext() *CompareContext {
	var p = new(CompareContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_compare
	return p
}

func (*CompareContext) IsCompareContext() {}

func NewCompareContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *CompareContext {
	var p = new(CompareContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_compare

	return p
}

func (s *CompareContext) GetParser() antlr.Parser { return s.parser }

func (s *CompareContext) K_LT() antlr.TerminalNode {
	return s.GetToken(CQLParserK_LT, 0)
}

func (s *CompareContext) K_BT() antlr.TerminalNode {
	return s.GetToken(CQLParserK_BT, 0)
}

func (s *CompareContext) K_EQ() antlr.TerminalNode {
	return s.GetToken(CQLParserK_EQ, 0)
}

func (s *CompareContext) K_LE() antlr.TerminalNode {
	return s.GetToken(CQLParserK_LE, 0)
}

func (s *CompareContext) K_BE() antlr.TerminalNode {
	return s.GetToken(CQLParserK_BE, 0)
}

func (s *CompareContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *CompareContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *CompareContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterCompare(s)
	}
}

func (s *CompareContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitCompare(s)
	}
}

func (s *CompareContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitCompare(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Compare() (localctx ICompareContext) {
	localctx = NewCompareContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 40, CQLParserRULE_compare)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	p.SetState(163)
	_la = p.GetTokenStream().LA(1)

	if !(((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<CQLParserK_LT)|(1<<CQLParserK_BT)|(1<<CQLParserK_EQ)|(1<<CQLParserK_LE)|(1<<CQLParserK_BE))) != 0) {
		p.GetErrorHandler().RecoverInline(p)
	} else {
		p.GetErrorHandler().ReportMatch(p)
		p.Consume()
	}

	return localctx
}

// IIntListContext is an interface to support dynamic dispatch.
type IIntListContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsIntListContext differentiates from other interfaces.
	IsIntListContext()
}

type IntListContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyIntListContext() *IntListContext {
	var p = new(IntListContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_intList
	return p
}

func (*IntListContext) IsIntListContext() {}

func NewIntListContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *IntListContext {
	var p = new(IntListContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_intList

	return p
}

func (s *IntListContext) GetParser() antlr.Parser { return s.parser }

func (s *IntListContext) AllINT() []antlr.TerminalNode {
	return s.GetTokens(CQLParserINT)
}

func (s *IntListContext) INT(i int) antlr.TerminalNode {
	return s.GetToken(CQLParserINT, i)
}

func (s *IntListContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IntListContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *IntListContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterIntList(s)
	}
}

func (s *IntListContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitIntList(s)
	}
}

func (s *IntListContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitIntList(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) IntList() (localctx IIntListContext) {
	localctx = NewIntListContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 42, CQLParserRULE_intList)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(165)
		p.Match(CQLParserT__10)
	}
	{
		p.SetState(166)
		p.Match(CQLParserINT)
	}
	p.SetState(171)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == CQLParserT__11 {
		{
			p.SetState(167)
			p.Match(CQLParserT__11)
		}
		{
			p.SetState(168)
			p.Match(CQLParserINT)
		}

		p.SetState(173)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(174)
		p.Match(CQLParserT__12)
	}

	return localctx
}

// ILimitContext is an interface to support dynamic dispatch.
type ILimitContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsLimitContext differentiates from other interfaces.
	IsLimitContext()
}

type LimitContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLimitContext() *LimitContext {
	var p = new(LimitContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = CQLParserRULE_limit
	return p
}

func (*LimitContext) IsLimitContext() {}

func NewLimitContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LimitContext {
	var p = new(LimitContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = CQLParserRULE_limit

	return p
}

func (s *LimitContext) GetParser() antlr.Parser { return s.parser }

func (s *LimitContext) INT() antlr.TerminalNode {
	return s.GetToken(CQLParserINT, 0)
}

func (s *LimitContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LimitContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *LimitContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.EnterLimit(s)
	}
}

func (s *LimitContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(CQLListener); ok {
		listenerT.ExitLimit(s)
	}
}

func (s *LimitContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case CQLVisitor:
		return t.VisitLimit(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *CQLParser) Limit() (localctx ILimitContext) {
	localctx = NewLimitContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 44, CQLParserRULE_limit)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(176)
		p.Match(CQLParserINT)
	}

	return localctx
}
