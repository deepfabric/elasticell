// Generated from /home/zhichyu/src/github.com/deepfabric/indexer/cql/parser/CQL.g4 by ANTLR 4.7.

package parser // CQL

import "github.com/antlr/antlr4/runtime/Go/antlr"

// CQLListener is a complete listener for a parse tree produced by CQLParser.
type CQLListener interface {
	antlr.ParseTreeListener

	// EnterCql is called when entering the cql production.
	EnterCql(c *CqlContext)

	// EnterCreate is called when entering the create production.
	EnterCreate(c *CreateContext)

	// EnterDestroy is called when entering the destroy production.
	EnterDestroy(c *DestroyContext)

	// EnterInsert is called when entering the insert production.
	EnterInsert(c *InsertContext)

	// EnterDel is called when entering the del production.
	EnterDel(c *DelContext)

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterIndexName is called when entering the indexName production.
	EnterIndexName(c *IndexNameContext)

	// EnterDocument is called when entering the document production.
	EnterDocument(c *DocumentContext)

	// EnterUintPropDef is called when entering the uintPropDef production.
	EnterUintPropDef(c *UintPropDefContext)

	// EnterEnumPropDef is called when entering the enumPropDef production.
	EnterEnumPropDef(c *EnumPropDefContext)

	// EnterStrPropDef is called when entering the strPropDef production.
	EnterStrPropDef(c *StrPropDefContext)

	// EnterOrderLimit is called when entering the orderLimit production.
	EnterOrderLimit(c *OrderLimitContext)

	// EnterOrder is called when entering the order production.
	EnterOrder(c *OrderContext)

	// EnterProperty is called when entering the property production.
	EnterProperty(c *PropertyContext)

	// EnterUintType is called when entering the uintType production.
	EnterUintType(c *UintTypeContext)

	// EnterDocId is called when entering the docId production.
	EnterDocId(c *DocIdContext)

	// EnterValue is called when entering the value production.
	EnterValue(c *ValueContext)

	// EnterUintPred is called when entering the uintPred production.
	EnterUintPred(c *UintPredContext)

	// EnterEnumPred is called when entering the enumPred production.
	EnterEnumPred(c *EnumPredContext)

	// EnterStrPred is called when entering the strPred production.
	EnterStrPred(c *StrPredContext)

	// EnterCompare is called when entering the compare production.
	EnterCompare(c *CompareContext)

	// EnterIntList is called when entering the intList production.
	EnterIntList(c *IntListContext)

	// EnterLimit is called when entering the limit production.
	EnterLimit(c *LimitContext)

	// ExitCql is called when exiting the cql production.
	ExitCql(c *CqlContext)

	// ExitCreate is called when exiting the create production.
	ExitCreate(c *CreateContext)

	// ExitDestroy is called when exiting the destroy production.
	ExitDestroy(c *DestroyContext)

	// ExitInsert is called when exiting the insert production.
	ExitInsert(c *InsertContext)

	// ExitDel is called when exiting the del production.
	ExitDel(c *DelContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitIndexName is called when exiting the indexName production.
	ExitIndexName(c *IndexNameContext)

	// ExitDocument is called when exiting the document production.
	ExitDocument(c *DocumentContext)

	// ExitUintPropDef is called when exiting the uintPropDef production.
	ExitUintPropDef(c *UintPropDefContext)

	// ExitEnumPropDef is called when exiting the enumPropDef production.
	ExitEnumPropDef(c *EnumPropDefContext)

	// ExitStrPropDef is called when exiting the strPropDef production.
	ExitStrPropDef(c *StrPropDefContext)

	// ExitOrderLimit is called when exiting the orderLimit production.
	ExitOrderLimit(c *OrderLimitContext)

	// ExitOrder is called when exiting the order production.
	ExitOrder(c *OrderContext)

	// ExitProperty is called when exiting the property production.
	ExitProperty(c *PropertyContext)

	// ExitUintType is called when exiting the uintType production.
	ExitUintType(c *UintTypeContext)

	// ExitDocId is called when exiting the docId production.
	ExitDocId(c *DocIdContext)

	// ExitValue is called when exiting the value production.
	ExitValue(c *ValueContext)

	// ExitUintPred is called when exiting the uintPred production.
	ExitUintPred(c *UintPredContext)

	// ExitEnumPred is called when exiting the enumPred production.
	ExitEnumPred(c *EnumPredContext)

	// ExitStrPred is called when exiting the strPred production.
	ExitStrPred(c *StrPredContext)

	// ExitCompare is called when exiting the compare production.
	ExitCompare(c *CompareContext)

	// ExitIntList is called when exiting the intList production.
	ExitIntList(c *IntListContext)

	// ExitLimit is called when exiting the limit production.
	ExitLimit(c *LimitContext)
}
