// Generated from /home/zhichyu/src/github.com/deepfabric/indexer/cql/parser/CQL.g4 by ANTLR 4.7.

package parser // CQL

import "github.com/antlr/antlr4/runtime/Go/antlr"

// A complete Visitor for a parse tree produced by CQLParser.
type CQLVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by CQLParser#cql.
	VisitCql(ctx *CqlContext) interface{}

	// Visit a parse tree produced by CQLParser#create.
	VisitCreate(ctx *CreateContext) interface{}

	// Visit a parse tree produced by CQLParser#destroy.
	VisitDestroy(ctx *DestroyContext) interface{}

	// Visit a parse tree produced by CQLParser#insert.
	VisitInsert(ctx *InsertContext) interface{}

	// Visit a parse tree produced by CQLParser#del.
	VisitDel(ctx *DelContext) interface{}

	// Visit a parse tree produced by CQLParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by CQLParser#indexName.
	VisitIndexName(ctx *IndexNameContext) interface{}

	// Visit a parse tree produced by CQLParser#document.
	VisitDocument(ctx *DocumentContext) interface{}

	// Visit a parse tree produced by CQLParser#uintPropDef.
	VisitUintPropDef(ctx *UintPropDefContext) interface{}

	// Visit a parse tree produced by CQLParser#enumPropDef.
	VisitEnumPropDef(ctx *EnumPropDefContext) interface{}

	// Visit a parse tree produced by CQLParser#strPropDef.
	VisitStrPropDef(ctx *StrPropDefContext) interface{}

	// Visit a parse tree produced by CQLParser#orderLimit.
	VisitOrderLimit(ctx *OrderLimitContext) interface{}

	// Visit a parse tree produced by CQLParser#order.
	VisitOrder(ctx *OrderContext) interface{}

	// Visit a parse tree produced by CQLParser#property.
	VisitProperty(ctx *PropertyContext) interface{}

	// Visit a parse tree produced by CQLParser#uintType.
	VisitUintType(ctx *UintTypeContext) interface{}

	// Visit a parse tree produced by CQLParser#docId.
	VisitDocId(ctx *DocIdContext) interface{}

	// Visit a parse tree produced by CQLParser#value.
	VisitValue(ctx *ValueContext) interface{}

	// Visit a parse tree produced by CQLParser#uintPred.
	VisitUintPred(ctx *UintPredContext) interface{}

	// Visit a parse tree produced by CQLParser#enumPred.
	VisitEnumPred(ctx *EnumPredContext) interface{}

	// Visit a parse tree produced by CQLParser#strPred.
	VisitStrPred(ctx *StrPredContext) interface{}

	// Visit a parse tree produced by CQLParser#compare.
	VisitCompare(ctx *CompareContext) interface{}

	// Visit a parse tree produced by CQLParser#intList.
	VisitIntList(ctx *IntListContext) interface{}

	// Visit a parse tree produced by CQLParser#limit.
	VisitLimit(ctx *LimitContext) interface{}
}
