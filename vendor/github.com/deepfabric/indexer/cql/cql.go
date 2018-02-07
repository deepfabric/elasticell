package cql

import (
	"fmt"
	"math"
	"strconv"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/deepfabric/indexer/cql/parser"
	"github.com/pkg/errors"
)

const (
	DEFAULT_LIMIT = 100
)

const (
	TypeUint8 = iota //0
	TypeUint16
	TypeUint32
	TypeUint64
	TypeEnum
	TypeStr
)

type UintPred struct {
	Name      string
	Low, High uint64
}

type EnumPred struct {
	Name   string
	InVals []int
}

type StrPred struct {
	Name     string
	ContWord string
}

type CqlCreate struct {
	DocumentWithIdx
}

type CqlDestroy struct {
	Index string
}

type CqlInsert struct {
	DocumentWithIdx
}

type CqlDel struct {
	DocumentWithIdx
}

type CqlSelect struct {
	Index     string
	UintPreds map[string]UintPred
	EnumPreds map[string]EnumPred
	StrPreds  map[string]StrPred
	OrderBy   string
	Limit     int
}

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

type myCqlVisitor struct {
	parser.BaseCQLVisitor
	docProts map[string]*Document
	res      interface{} //record the intermediate and final result of visitor
	index    string      //index name
}

func (v *myCqlVisitor) VisitCql(ctx *parser.CqlContext) (err interface{}) {
	//If there are multiple subrules, then check one by one.
	if create := ctx.Create(); create != nil {
		err = v.VisitCreate(create.(*parser.CreateContext))
	} else if destroy := ctx.Destroy(); destroy != nil {
		err = v.VisitDestroy(destroy.(*parser.DestroyContext))
	} else if ins := ctx.Insert(); ins != nil {
		err = v.VisitInsert(ins.(*parser.InsertContext))
	} else if del := ctx.Del(); del != nil {
		err = v.VisitDel(del.(*parser.DelContext))
	} else if query := ctx.Query(); query != nil {
		err = v.VisitQuery(query.(*parser.QueryContext))
	} else {
		err = errors.Errorf("unsupported subrule of cql")
	}
	//v.res has already be populated
	return
}

func (v *myCqlVisitor) VisitCreate(ctx *parser.CreateContext) (err interface{}) {
	q := &CqlCreate{}
	q.Index = ctx.IndexName().GetText()
	for _, popDef := range ctx.AllUintPropDef() {
		if err = v.VisitUintPropDef(popDef.(*parser.UintPropDefContext)); err != nil {
			return
		}
		q.Doc.UintProps = append(q.Doc.UintProps, v.res.(*UintProp))
	}
	for _, popDef := range ctx.AllEnumPropDef() {
		if err = v.VisitEnumPropDef(popDef.(*parser.EnumPropDefContext)); err != nil {
			return
		}
		q.Doc.EnumProps = append(q.Doc.EnumProps, v.res.(*EnumProp))
	}
	for _, popDef := range ctx.AllStrPropDef() {
		if err = v.VisitStrPropDef(popDef.(*parser.StrPropDefContext)); err != nil {
			return
		}
		q.Doc.StrProps = append(q.Doc.StrProps, v.res.(*StrProp))
	}
	v.res = q
	return
}

func (v *myCqlVisitor) VisitUintPropDef(ctx *parser.UintPropDefContext) (err interface{}) {
	var pop UintProp
	pop.Name = ctx.Property().GetText()
	uintType := ctx.UintType().(*parser.UintTypeContext)
	if u8 := uintType.K_UINT8(); u8 != nil {
		pop.ValLen = 1
	} else if u16 := uintType.K_UINT16(); u16 != nil {
		pop.ValLen = 2
	} else if u32 := uintType.K_UINT32(); u32 != nil {
		pop.ValLen = 4
	} else if u64 := uintType.K_UINT64(); u64 != nil {
		pop.ValLen = 8
	} else if u32 := uintType.K_FLOAT32(); u32 != nil {
		pop.IsFloat = true
		pop.ValLen = 4
	} else if u64 := uintType.K_FLOAT64(); u64 != nil {
		pop.IsFloat = true
		pop.ValLen = 8
	} else {
		panic(fmt.Sprintf("invalid uintType: %v\n", ctx.UintType().GetText()))
	}
	v.res = &pop
	return
}

func (v *myCqlVisitor) VisitEnumPropDef(ctx *parser.EnumPropDefContext) (err interface{}) {
	var pop EnumProp
	pop.Name = ctx.Property().GetText()
	v.res = &pop
	return
}

func (v *myCqlVisitor) VisitStrPropDef(ctx *parser.StrPropDefContext) (err interface{}) {
	var pop StrProp
	pop.Name = ctx.Property().GetText()
	v.res = &pop
	return
}

func (v *myCqlVisitor) VisitDestroy(ctx *parser.DestroyContext) (err interface{}) {
	q := &CqlDestroy{}
	q.Index = ctx.IndexName().GetText()
	v.res = q
	return
}

func (v *myCqlVisitor) VisitInsert(ctx *parser.InsertContext) (err interface{}) {
	if err = v.VisitDocument(ctx.Document().(*parser.DocumentContext)); err != nil {
		return
	}
	q := &CqlInsert{} //TODO: better way to copy doc?
	q.DocumentWithIdx = *(v.res.(*DocumentWithIdx))
	v.res = q
	return
}

func (v *myCqlVisitor) VisitDel(ctx *parser.DelContext) (err interface{}) {
	if err = v.VisitDocument(ctx.Document().(*parser.DocumentContext)); err != nil {
		return
	}
	q := &CqlDel{} //TODO: better way to copy doc?
	q.DocumentWithIdx = *(v.res.(*DocumentWithIdx))
	v.res = q
	return
}

func (v *myCqlVisitor) VisitDocument(ctx *parser.DocumentContext) (err interface{}) {
	index := ctx.IndexName().GetText()
	docProt, ok := v.docProts[index]
	want := len(docProt.UintProps) + len(docProt.EnumProps) + len(docProt.StrProps)
	if !ok {
		err = errors.Errorf("failed to find the definion of index %s\n", index)
		return
	} else if len(ctx.AllValue()) != want {
		err = errors.Errorf("invalid number of values, is %d, want %d\n", len(ctx.AllValue()), want)
		return
	}
	doc := &DocumentWithIdx{}
	doc.Index = ctx.IndexName().GetText()
	var tmpU64 uint64
	var tmpInt int
	tmpU64, err = strconv.ParseUint(ctx.DocId().GetText(), 10, 64)
	if err != nil {
		err = errors.Wrap(err.(error), "")
		return
	}
	doc.Doc.DocID = tmpU64

	vals := ctx.AllValue()
	for i := 0; i < len(docProt.UintProps); i++ {
		if tmpU64, err = ParseUintProp(docProt.UintProps[i], vals[i].GetText()); err != nil {
			return
		}
		uintProp := docProt.UintProps[i]
		uintProp.Val = tmpU64
		doc.Doc.UintProps = append(doc.Doc.UintProps, uintProp)
	}
	for i := 0; i < len(docProt.EnumProps); i++ {
		tmpInt, err = strconv.Atoi(vals[i+len(docProt.UintProps)].GetText())
		if err != nil {
			err = errors.Wrap(err.(error), "")
			return
		}
		enumProp := docProt.EnumProps[i]
		enumProp.Val = uint64(tmpInt)
		doc.Doc.EnumProps = append(doc.Doc.EnumProps, enumProp)
	}
	for i := 0; i < len(docProt.StrProps); i++ {
		strProp := docProt.StrProps[i]
		strProp.Val = vals[i+len(docProt.UintProps)+len(docProt.EnumProps)].GetText()
		doc.Doc.StrProps = append(doc.Doc.StrProps, strProp)
	}
	v.res = doc
	return
}

func (v *myCqlVisitor) VisitQuery(ctx *parser.QueryContext) (err interface{}) {
	v.index = ctx.IndexName().GetText()
	q := &CqlSelect{
		Index:     ctx.IndexName().GetText(),
		UintPreds: make(map[string]UintPred),
		EnumPreds: make(map[string]EnumPred),
		StrPreds:  make(map[string]StrPred),
	}

	for i, predCtx := range ctx.AllUintPred() {
		if err = v.VisitUintPred(predCtx.(*parser.UintPredContext)); err != nil {
			return
		}
		uintPred := *(v.res.(*UintPred))
		uintPred2, ok := q.UintPreds[uintPred.Name]
		if ok {
			//fold multiple UintPred of the same property into one
			uintPred.Low = maxU64(uintPred.Low, uintPred2.Low)
			uintPred.High = minU64(uintPred.High, uintPred2.High)
		}
		q.UintPreds[uintPred.Name] = uintPred
		if i == 0 {
			q.OrderBy = uintPred.Name
		}
	}
	if ordCtx := ctx.OrderLimit(); ordCtx != nil {
		if err = v.VisitOrderLimit(ordCtx.(*parser.OrderLimitContext)); err != nil {
			return
		}
		ol := *(v.res.(*orderLimit))
		q.OrderBy = ol.order
		if _, ok := q.UintPreds[q.OrderBy]; !ok {
			err = errors.Errorf("invalid ORDERBY property %s, want a UintProp property", q.OrderBy)
			return
		}
		q.Limit = ol.limit
	} else if q.OrderBy != "" {
		q.Limit = DEFAULT_LIMIT
	}
	if q.OrderBy != "" {
		if _, ok := q.UintPreds[q.OrderBy]; !ok {
			err = errors.Errorf("invalid ORDERBY property %s, want a UintProp property", q.OrderBy)
			return
		}
	}
	for _, predCtx := range ctx.AllEnumPred() {
		if err = v.VisitEnumPred(predCtx.(*parser.EnumPredContext)); err != nil {
			return
		}
		enumPred := *(v.res.(*EnumPred))
		if _, ok := q.EnumPreds[enumPred.Name]; ok {
			err = errors.Errorf("invalid query due to multiple EnumPred of property %s", enumPred.Name)
			return
		}
		q.EnumPreds[enumPred.Name] = enumPred
	}
	for _, predCtx := range ctx.AllStrPred() {
		if err = v.VisitStrPred(predCtx.(*parser.StrPredContext)); err != nil {
			return
		}
		strPred := *(v.res.(*StrPred))
		if _, ok := q.StrPreds[strPred.Name]; ok {
			err = errors.Errorf("invalid query due to multiple StrPred of property %s", strPred.Name)
			return
		}
		q.StrPreds[strPred.Name] = strPred
	}
	v.res = q
	return
}

//https://stackoverflow.com/questions/27516387/what-is-the-correct-way-to-find-the-min-between-two-integers-in-go
func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

//https://stackoverflow.com/questions/44222554/how-to-remove-quotes-from-around-a-string-in-golang
func stripQuote(s string) string {
	if len(s) > 0 && s[0] == '"' {
		s = s[1:]
	}
	if len(s) > 0 && s[len(s)-1] == '"' {
		s = s[:len(s)-1]
	}
	return s
}

func (v *myCqlVisitor) VisitUintPred(ctx *parser.UintPredContext) (err interface{}) {
	var val uint64
	var docProt *Document
	var uintProp *UintProp
	var ok bool
	pred := &UintPred{Low: 0, High: ^uint64(0)}
	pred.Name = ctx.Property().GetText()
	if docProt, ok = v.docProts[v.index]; !ok {
		err = errors.Errorf("cannot find docProt for index %s", v.index)
		return
	}
	ok = false
	for _, uintProp = range docProt.UintProps {
		if uintProp.Name == pred.Name {
			ok = true
			break
		}
	}
	if !ok {
		err = errors.Errorf("cannot find UintPred %s in index %s", pred.Name, v.index)
		return

	}
	if val, err = ParseUintProp(uintProp, ctx.Value().GetText()); err != nil {
		return
	}
	cmp := ctx.Compare().(*parser.CompareContext)
	if lt := cmp.K_LT(); lt != nil {
		pred.High = val - 1
	} else if bt := cmp.K_BT(); bt != nil {
		pred.Low = val + 1
	} else if eq := cmp.K_EQ(); eq != nil {
		pred.Low = val
		pred.High = val
	} else if le := cmp.K_LE(); le != nil {
		pred.High = val
	} else if be := cmp.K_BE(); be != nil {
		pred.Low = val
	} else {
		panic(fmt.Sprintf("invalid compare: %v\n", cmp.GetText()))
	}
	v.res = pred
	return
}

func (v *myCqlVisitor) VisitEnumPred(ctx *parser.EnumPredContext) (err interface{}) {
	pred := &EnumPred{}
	pred.Name = ctx.Property().GetText()
	var docProt *Document
	var enumProp *EnumProp
	var ok bool
	if docProt, ok = v.docProts[v.index]; !ok {
		err = errors.Errorf("cannot find docProt for index %s", v.index)
		return
	}
	ok = false
	for _, enumProp = range docProt.EnumProps {
		if enumProp.Name == pred.Name {
			ok = true
			break
		}
	}
	if !ok {
		err = errors.Errorf("cannot find EnumPred %s in index %s", pred.Name, v.index)
		return

	}
	if err = v.VisitIntList(ctx.IntList().(*parser.IntListContext)); err != nil {
		return
	}
	pred.InVals = v.res.([]int)
	v.res = pred
	return
}

func (v *myCqlVisitor) VisitIntList(ctx *parser.IntListContext) (err interface{}) {
	intList := make([]int, 0, len(ctx.AllINT()))
	var val int
	for _, it := range ctx.AllINT() {
		val, err = strconv.Atoi(it.GetText())
		if err != nil {
			err = errors.Wrap(err.(error), "")
			return
		}
		intList = append(intList, val)
	}
	v.res = intList
	return
}

func (v *myCqlVisitor) VisitStrPred(ctx *parser.StrPredContext) (err interface{}) {
	pred := &StrPred{}
	pred.Name = ctx.Property().GetText()
	var docProt *Document
	var strProp *StrProp
	var ok bool
	if docProt, ok = v.docProts[v.index]; !ok {
		err = errors.Errorf("cannot find docProt for index %s", v.index)
		return
	}
	ok = false
	for _, strProp = range docProt.StrProps {
		if strProp.Name == pred.Name {
			ok = true
			break
		}
	}
	if !ok {
		err = errors.Errorf("cannot find StrPred %s in index %s", pred.Name, v.index)
		return

	}
	pred.ContWord = stripQuote(ctx.STRING().GetText())
	v.res = pred
	return
}

type orderLimit struct {
	order string
	limit int
}

func (v *myCqlVisitor) VisitOrderLimit(ctx *parser.OrderLimitContext) (err interface{}) {
	var ol orderLimit
	if ordCtx := ctx.Order(); ordCtx != nil {
		ol.order = ordCtx.GetText()
	}
	ol.limit = DEFAULT_LIMIT
	if lmtCtx := ctx.Limit(); lmtCtx != nil {
		ol.limit, err = strconv.Atoi(lmtCtx.GetText())
		if err != nil {
			err = errors.Wrap(err.(error), "")
			return
		}
	}
	v.res = &ol
	return
}

/*
Float32ToSortableUint64 converts a float32 string to sortable uint64.

Refers to:
github.com/apache/lucene-solr/lucene/core/src/java/org/apache/lucene/util/NumericUtils.java,
  public static int floatToSortableInt(float value);
  public static long doubleToSortableLong(double value);

https://en.wikipedia.org/wiki/Single-precision_floating-point_format
https://en.wikipedia.org/wiki/Double-precision_floating-point_format
*/
func Float32ToSortableUint64(valS string) (val uint64, err error) {
	var valF float64
	if valF, err = strconv.ParseFloat(valS, 32); err != nil {
		return
	}
	bits := math.Float32bits(float32(valF))
	int0 := int32(bits)
	val = uint64(uint32(int0^((int0>>31)&0x7fffffff)) ^ 0x80000000)
	return
}

//Float64ToSortableUint64 converts a float64 string to sortable uint64.
func Float64ToSortableUint64(valS string) (val uint64, err error) {
	var valF float64
	if valF, err = strconv.ParseFloat(valS, 64); err != nil {
		return
	}
	bits := math.Float64bits(valF)
	int0 := int64(bits)
	val = uint64(int0^((int0>>63)&0x7fffffffffffffff)) ^ 0x8000000000000000
	return
}

//ParseUintProp parses valS
func ParseUintProp(uintProp *UintProp, valS string) (val uint64, err error) {
	if uintProp.IsFloat {
		if uintProp.ValLen == 4 {
			if val, err = Float32ToSortableUint64(valS); err != nil {
				err = errors.Wrap(err.(error), "")
				return
			}
		} else {
			if val, err = Float64ToSortableUint64(valS); err != nil {
				err = errors.Wrap(err.(error), "")
				return
			}
		}
	} else {
		val, err = strconv.ParseUint(valS, 10, 64)
		if err != nil {
			err = errors.Wrap(err.(error), "")
			return
		}
	}
	return
}

//ParseCql parse CQL. res type is one of CqlCreate/CqlDestroy/CqlInsert/CqlDel/CqlQuery.
func ParseCql(cql string, docProts map[string]*Document) (res interface{}, err error) {
	input := antlr.NewInputStream(cql)
	lexer := parser.NewCQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)
	parser := parser.NewCQLParser(stream)
	el := new(VerboseErrorListener)
	parser.AddErrorListener(el)

	tree := parser.Cql()
	if el.err != nil {
		err = el.err
		return
	}

	visitor := &myCqlVisitor{docProts: docProts}
	if err1 := tree.Accept(visitor); err1 != nil {
		err = err1.(error) //panic: interface conversion: interface is nil, not error
		return
	}
	res = visitor.res
	return
}
