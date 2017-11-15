// Generated from /home/zhichyu/src/github.com/deepfabric/indexer/cql/parser/CQL.g4 by ANTLR 4.7.

package parser // CQL

import "github.com/antlr/antlr4/runtime/Go/antlr"

// BaseCQLListener is a complete listener for a parse tree produced by CQLParser.
type BaseCQLListener struct{}

var _ CQLListener = &BaseCQLListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseCQLListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseCQLListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseCQLListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseCQLListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterCql is called when production cql is entered.
func (s *BaseCQLListener) EnterCql(ctx *CqlContext) {}

// ExitCql is called when production cql is exited.
func (s *BaseCQLListener) ExitCql(ctx *CqlContext) {}

// EnterCreate is called when production create is entered.
func (s *BaseCQLListener) EnterCreate(ctx *CreateContext) {}

// ExitCreate is called when production create is exited.
func (s *BaseCQLListener) ExitCreate(ctx *CreateContext) {}

// EnterDestroy is called when production destroy is entered.
func (s *BaseCQLListener) EnterDestroy(ctx *DestroyContext) {}

// ExitDestroy is called when production destroy is exited.
func (s *BaseCQLListener) ExitDestroy(ctx *DestroyContext) {}

// EnterInsert is called when production insert is entered.
func (s *BaseCQLListener) EnterInsert(ctx *InsertContext) {}

// ExitInsert is called when production insert is exited.
func (s *BaseCQLListener) ExitInsert(ctx *InsertContext) {}

// EnterDel is called when production del is entered.
func (s *BaseCQLListener) EnterDel(ctx *DelContext) {}

// ExitDel is called when production del is exited.
func (s *BaseCQLListener) ExitDel(ctx *DelContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseCQLListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseCQLListener) ExitQuery(ctx *QueryContext) {}

// EnterIndexName is called when production indexName is entered.
func (s *BaseCQLListener) EnterIndexName(ctx *IndexNameContext) {}

// ExitIndexName is called when production indexName is exited.
func (s *BaseCQLListener) ExitIndexName(ctx *IndexNameContext) {}

// EnterDocument is called when production document is entered.
func (s *BaseCQLListener) EnterDocument(ctx *DocumentContext) {}

// ExitDocument is called when production document is exited.
func (s *BaseCQLListener) ExitDocument(ctx *DocumentContext) {}

// EnterUintPropDef is called when production uintPropDef is entered.
func (s *BaseCQLListener) EnterUintPropDef(ctx *UintPropDefContext) {}

// ExitUintPropDef is called when production uintPropDef is exited.
func (s *BaseCQLListener) ExitUintPropDef(ctx *UintPropDefContext) {}

// EnterEnumPropDef is called when production enumPropDef is entered.
func (s *BaseCQLListener) EnterEnumPropDef(ctx *EnumPropDefContext) {}

// ExitEnumPropDef is called when production enumPropDef is exited.
func (s *BaseCQLListener) ExitEnumPropDef(ctx *EnumPropDefContext) {}

// EnterStrPropDef is called when production strPropDef is entered.
func (s *BaseCQLListener) EnterStrPropDef(ctx *StrPropDefContext) {}

// ExitStrPropDef is called when production strPropDef is exited.
func (s *BaseCQLListener) ExitStrPropDef(ctx *StrPropDefContext) {}

// EnterOrderLimit is called when production orderLimit is entered.
func (s *BaseCQLListener) EnterOrderLimit(ctx *OrderLimitContext) {}

// ExitOrderLimit is called when production orderLimit is exited.
func (s *BaseCQLListener) ExitOrderLimit(ctx *OrderLimitContext) {}

// EnterOrder is called when production order is entered.
func (s *BaseCQLListener) EnterOrder(ctx *OrderContext) {}

// ExitOrder is called when production order is exited.
func (s *BaseCQLListener) ExitOrder(ctx *OrderContext) {}

// EnterProperty is called when production property is entered.
func (s *BaseCQLListener) EnterProperty(ctx *PropertyContext) {}

// ExitProperty is called when production property is exited.
func (s *BaseCQLListener) ExitProperty(ctx *PropertyContext) {}

// EnterUintType is called when production uintType is entered.
func (s *BaseCQLListener) EnterUintType(ctx *UintTypeContext) {}

// ExitUintType is called when production uintType is exited.
func (s *BaseCQLListener) ExitUintType(ctx *UintTypeContext) {}

// EnterDocId is called when production docId is entered.
func (s *BaseCQLListener) EnterDocId(ctx *DocIdContext) {}

// ExitDocId is called when production docId is exited.
func (s *BaseCQLListener) ExitDocId(ctx *DocIdContext) {}

// EnterValue is called when production value is entered.
func (s *BaseCQLListener) EnterValue(ctx *ValueContext) {}

// ExitValue is called when production value is exited.
func (s *BaseCQLListener) ExitValue(ctx *ValueContext) {}

// EnterUintPred is called when production uintPred is entered.
func (s *BaseCQLListener) EnterUintPred(ctx *UintPredContext) {}

// ExitUintPred is called when production uintPred is exited.
func (s *BaseCQLListener) ExitUintPred(ctx *UintPredContext) {}

// EnterEnumPred is called when production enumPred is entered.
func (s *BaseCQLListener) EnterEnumPred(ctx *EnumPredContext) {}

// ExitEnumPred is called when production enumPred is exited.
func (s *BaseCQLListener) ExitEnumPred(ctx *EnumPredContext) {}

// EnterStrPred is called when production strPred is entered.
func (s *BaseCQLListener) EnterStrPred(ctx *StrPredContext) {}

// ExitStrPred is called when production strPred is exited.
func (s *BaseCQLListener) ExitStrPred(ctx *StrPredContext) {}

// EnterCompare is called when production compare is entered.
func (s *BaseCQLListener) EnterCompare(ctx *CompareContext) {}

// ExitCompare is called when production compare is exited.
func (s *BaseCQLListener) ExitCompare(ctx *CompareContext) {}

// EnterIntList is called when production intList is entered.
func (s *BaseCQLListener) EnterIntList(ctx *IntListContext) {}

// ExitIntList is called when production intList is exited.
func (s *BaseCQLListener) ExitIntList(ctx *IntListContext) {}

// EnterLimit is called when production limit is entered.
func (s *BaseCQLListener) EnterLimit(ctx *LimitContext) {}

// ExitLimit is called when production limit is exited.
func (s *BaseCQLListener) ExitLimit(ctx *LimitContext) {}
