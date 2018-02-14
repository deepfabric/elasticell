package parser // CQL
import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/pkg/errors"
)

type VerboseErrorListener struct {
	antlr.DefaultErrorListener
	err error
}

func (el *VerboseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	parser := recognizer.(antlr.Parser)
	stack := parser.GetRuleInvocationStack(parser.GetParserRuleContext())
	el.err = errors.Errorf("rule stack: %v, line %d:%d at %v: %s\n", stack, line, column, offendingSymbol, msg)
}

func (el *VerboseErrorListener) ReportAmbiguity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, exact bool, ambigAlts *antlr.BitSet, configs antlr.ATNConfigSet) {
	parser := recognizer.(antlr.Parser)
	stack := parser.GetRuleInvocationStack(parser.GetParserRuleContext())
	el.err = errors.Errorf("rule stack: %v, ReportAmbiguity %v %v %v %v %v\n", stack, startIndex, stopIndex, exact, ambigAlts, configs)
}

func (el *VerboseErrorListener) ReportAttemptingFullContext(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, conflictingAlts *antlr.BitSet, configs antlr.ATNConfigSet) {
	parser := recognizer.(antlr.Parser)
	stack := parser.GetRuleInvocationStack(parser.GetParserRuleContext())
	el.err = errors.Errorf("rule stack: %v, ReportAttemptingFullContext %v %v %v %v\n", stack, startIndex, stopIndex, conflictingAlts, configs)
}

func (el *VerboseErrorListener) ReportContextSensitivity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex, prediction int, configs antlr.ATNConfigSet) {
	parser := recognizer.(antlr.Parser)
	stack := parser.GetRuleInvocationStack(parser.GetParserRuleContext())
	el.err = errors.Errorf("rule stack: %v, ReportContextSensitivity %v %v %v %v\n", stack, startIndex, stopIndex, prediction, configs)
}

type ParserCase struct {
	Input       string
	ExpectError bool
}

func TestCqlParserError(t *testing.T) {
	fmt.Println("================TestCqlParserError================")
	tcs := []ParserCase{
		//normal case
		{"IDX.CREATE orders SCHEMA object UINT64 price UINT32 number UINT32 date UINT64 type ENUM desc STRING", false},
		//invalid token: "IDX"
		{"IDX orders SCHEMA object UINT64 price FLOAT number UINT32 date UINT64", true},
		//invalid order of "desc STRING type ENUM"
		{"IDX.CREATE orders SCHEMA object UINT64 price UINT32 number UINT32 date UINT64 desc STRING type ENUM", true},
		//invalid query due to LIMIT without ORDERBY
		{"IDX.SELECT orders WHERE price>=30 price<=40 type IN [1,3] LIMIT 30", true},
	}
	for i, tc := range tcs {
		input := antlr.NewInputStream(tc.Input)
		lexer := NewCQLLexer(input)
		stream := antlr.NewCommonTokenStream(lexer, 0)
		parser := NewCQLParser(stream)

		el := new(VerboseErrorListener)
		parser.AddErrorListener(el)
		_ = parser.Cql()

		fmt.Println(input)
		if el.err != nil {
			fmt.Printf("parser raised exception %+v\n", el.err)
		}
		if (tc.ExpectError && el.err == nil) || (!tc.ExpectError && el.err != nil) {
			t.Fatalf("case %d failed. have %v, want %v", i, !tc.ExpectError, tc.ExpectError)
		}
	}
}

type CqlTestListener struct {
	BaseCQLListener
}

func NewCqlTestListener() *CqlTestListener {
	return new(CqlTestListener)
}

/*func (l *CqlTestListener) EnterEveryRule(ctx antlr.ParserRuleContext) {
	fmt.Println(ctx.GetText())
}*/

func (l *CqlTestListener) VisitTerminal(node antlr.TerminalNode) {
	fmt.Printf("VisitTerminal: %v, tokenType: %v\n", node.GetText(), node.GetSymbol().GetTokenType())
}

func (l *CqlTestListener) EnterCql(ctx *CqlContext) {
	fmt.Printf("EnterCql: %v\n", ctx.GetText())
}

func (l *CqlTestListener) ExitCreate(ctx *CreateContext) {
	fmt.Printf("ExitCreate: %v\n", ctx.GetText())
	fmt.Printf("create indexName: %v\n", ctx.IndexName().GetText())
}

//POC of listener. Printing every token's type helps to find grammer ambiguity.
func TestCqlListener(t *testing.T) {
	fmt.Println("================TestCqlListener================")
	tcs := []string{
		"IDX.CREATE orders SCHEMA object UINT64 price FLOAT number UINT32 date UINT64 desc STRING",
		"IDX.INSERT orders 615 11 22 33 44 \"description\"",
		"IDX.SELECT orders WHERE price>=30 price<40 date<2017 type IN [1,3] desc CONTAINS \"pen\" ORDERBY date",
	}
	for _, tc := range tcs {
		input := antlr.NewInputStream(tc)
		lexer := NewCQLLexer(input)
		stream := antlr.NewCommonTokenStream(lexer, 0)
		parser := NewCQLParser(stream)
		//parser.AddErrorListener(antlr.NewDiagnosticErrorListener(true))
		//parser.BuildParseTrees = true
		tree := parser.Cql()
		listener := NewCqlTestListener()
		antlr.ParseTreeWalkerDefault.Walk(listener, tree)
	}
}

type CqlTestVisitor struct {
	BaseCQLVisitor
	res interface{} //record the result of visitor
}

func (v *CqlTestVisitor) VisitCql(ctx *CqlContext) interface{} {
	fmt.Printf("VisitCql %v...\n", ctx)
	//If there are multiple subrules, then check one by one.
	if create := ctx.Create(); create != nil {
		v.res = v.VisitCreate(create.(*CreateContext))
	} else if destroy := ctx.Destroy(); destroy != nil {
		v.res = v.VisitDestroy(destroy.(*DestroyContext))
	}
	return nil
}

type UintProp struct {
	Name   string
	ValLen int //one of 1, 2, 4, 8
	Val    uint64
}

type EnumProp struct {
	Name string
	Val  int
}

type StrProp struct {
	Name string
	Val  string
}

type Document struct {
	UintProps []UintProp
	EnumProps []EnumProp
	StrProps  []StrProp
}

func (v *CqlTestVisitor) VisitCreate(ctx *CreateContext) interface{} {
	fmt.Println("VisitCreate...")
	indexName := ctx.IndexName().GetText()
	fmt.Printf("indexName: %s\n", indexName)
	var doc Document
	for _, popDef := range ctx.AllUintPropDef() {
		pop := v.VisitUintPropDef(popDef.(*UintPropDefContext))
		if pop.(UintProp).ValLen == 0 {
			continue
		}
		doc.UintProps = append(doc.UintProps, pop.(UintProp))
	}
	for _, popDef := range ctx.AllEnumPropDef() {
		pop := v.VisitEnumPropDef(popDef.(*EnumPropDefContext))
		doc.EnumProps = append(doc.EnumProps, pop.(EnumProp))
	}
	for _, popDef := range ctx.AllStrPropDef() {
		pop := v.VisitStrPropDef(popDef.(*StrPropDefContext))
		doc.StrProps = append(doc.StrProps, pop.(StrProp))
	}
	return fmt.Sprintf("Create index %s schema %v\n", indexName, doc)
}

func (v *CqlTestVisitor) VisitUintPropDef(ctx *UintPropDefContext) interface{} {
	fmt.Println("VisitUintPropDef...")
	pop := UintProp{}
	pop.Name = ctx.Property().GetText()
	uintType := ctx.UintType().(*UintTypeContext)
	if u8 := uintType.K_UINT8(); u8 != nil {
		pop.ValLen = 1
	} else if u16 := uintType.K_UINT16(); u16 != nil {
		pop.ValLen = 2
	} else if u32 := uintType.K_UINT32(); u32 != nil {
		pop.ValLen = 4
	} else if u64 := uintType.K_UINT64(); u64 != nil {
		pop.ValLen = 8
	} else {
		panic(fmt.Sprintf("invalid uintType: %v", ctx.UintType().GetText()))
	}
	return pop
}

func (v *CqlTestVisitor) VisitEnumPropDef(ctx *EnumPropDefContext) interface{} {
	fmt.Println("VisitEnumPropDef...")
	pop := EnumProp{}
	pop.Name = ctx.Property().GetText()
	return pop
}

func (v *CqlTestVisitor) VisitStrPropDef(ctx *StrPropDefContext) interface{} {
	fmt.Println("VisitStrPropDef...")
	pop := StrProp{}
	pop.Name = ctx.Property().GetText()
	return pop
}

func (v *CqlTestVisitor) VisitDestroy(ctx *DestroyContext) interface{} {
	fmt.Println("VisitDestroy...")
	indexName := ctx.IndexName().GetText()
	fmt.Printf("indexName: %s\n", indexName)
	return fmt.Sprintf("Destroy %s", indexName)
}

//POC of visitor
func TestCqlVisitor(t *testing.T) {
	fmt.Println("================TestCqlVisitor================")

	input := antlr.NewInputStream("IDX.CREATE orders SCHEMA object UINT64 number UINT32 date UINT64 price UINT16 desc STRING")
	//input := antlr.NewInputStream("IDX.DESTROY orders")
	lexer := NewCQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)
	parser := NewCQLParser(stream)
	el := new(VerboseErrorListener)
	parser.AddErrorListener(el)
	//parser.BuildParseTrees = true

	tree := parser.Cql()
	require.NoErrorf(t, el.err, "parser raised exception")

	visitor := new(CqlTestVisitor)
	tree.Accept(visitor)
	fmt.Printf("the result of visitor: %v\n", visitor.res)
}
