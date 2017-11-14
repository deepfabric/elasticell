// Generated from /home/zhichyu/src/github.com/deepfabric/indexer/cql/parser/CQL.g4 by ANTLR 4.7.

package parser // CQL

import "github.com/antlr/antlr4/runtime/Go/antlr"

type BaseCQLVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseCQLVisitor) VisitCql(ctx *CqlContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitCreate(ctx *CreateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitDestroy(ctx *DestroyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitInsert(ctx *InsertContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitDel(ctx *DelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitIndexName(ctx *IndexNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitDocument(ctx *DocumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitUintPropDef(ctx *UintPropDefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitEnumPropDef(ctx *EnumPropDefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitStrPropDef(ctx *StrPropDefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitOrderLimit(ctx *OrderLimitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitOrder(ctx *OrderContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitProperty(ctx *PropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitUintType(ctx *UintTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitDocId(ctx *DocIdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitValue(ctx *ValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitUintPred(ctx *UintPredContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitEnumPred(ctx *EnumPredContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitStrPred(ctx *StrPredContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitCompare(ctx *CompareContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitIntList(ctx *IntListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseCQLVisitor) VisitLimit(ctx *LimitContext) interface{} {
	return v.VisitChildren(ctx)
}
