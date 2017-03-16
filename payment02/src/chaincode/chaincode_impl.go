package main

import (
	"errors"
	"fmt"
	"log"
	"payment02/src/util"
	"payment02/src/wrapper"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

type PaymentChaincode struct {
}

//初始化创建表
func (t *PaymentChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	defer util.End(util.Begin("Init"))

	log.Println("Init method.........")
	return nil, util.CreateTable(stub)
}

//调用方法
func (t *PaymentChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	defer util.End(util.Begin("Invoke"))

	if function == "InitData" { //初始化数据
		log.Println("InitData,args = " + args[0])
		return wrapper.InitData(stub, args)
	} else if function == "Payment" { //汇出银行汇款
		log.Println("Payment,args = " + args[0])
		return wrapper.Payment(stub, args)
	} else if function == "UpdateTransferStatus" { //汇入银行得到中央银行核心系统提示改变状态
		log.Println("UpdateTransferStatus,args = " + args[0])
		return wrapper.UpdateTransferStatus(stub, args)
	}
	return nil, errors.New("调用invoke失败")
}

//查询
func (t *PaymentChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	defer util.End(util.Begin("Query"))

	if function == "QueryTdNoStroBalRecordByKey" { //用户根据主键查询账户表记录(NOSTRO表)
		log.Println("QueryTdNoStroBalRecordByKey,args = " + args[0])
		fmt.Println("QueryTdNoStroBalRecordByKey,args = " + args[0])
		return wrapper.QueryTdNoStroBalRecordByKey(stub, args)
	} else if function == "SelectAllTxInstr" { //银行查询所有满足银行交易的数据（新增的N）
		log.Println("SelectAllTxInstr,args = " + args[0])
		fmt.Println("SelectAllTxInstr,args = " + args[0])
		return wrapper.SelectAllTxInstr(stub, args)
	} else if function == "SelectSimpleAllTxInstrIn" { //用户查询所有满足个人用户交易的数据In(转入)
		log.Println("SelectSimpleAllTxInstr,args = " + args[0])
		fmt.Println("SelectSimpleAllTxInstr,args = " + args[0])
		return wrapper.SelectSimpleAllTxInstrIn(stub, args)
	} else if function == "SelectSimpleAllTxInstrOut" { //用户查询所有满足个人用户交易的数据Out（转出）
		log.Println("SelectSimpleAllTxInstrOut,args = " + args[0])
		fmt.Println("SelectSimpleAllTxInstrOut,args = " + args[0])
		return wrapper.SelectSimpleAllTxInstrOut(stub, args)
	} else if function == "SelectBlankAllTxInstrOut" { //用户查询所有数据银行（转出）
		log.Println("SelectBlankAllTxInstrOut,args = " + args[0])
		fmt.Println("SelectBlankAllTxInstrOut,args = " + args[0])
		return wrapper.SelectBlankAllTxInstrOut(stub, args)
	} else if function == "SelectBlankAllTxInstrIn" { //用户查询所有数据银行（转入）
		log.Println("SelectBlankAllTxInstrIn,args = " + args[0])
		fmt.Println("SelectBlankAllTxInstrIn,args = " + args[0])
		return wrapper.SelectBlankAllTxInstrIn(stub, args)
	}

	return nil, errors.New("调用query失败")
}

func main() {

	err := shim.Start(new(PaymentChaincode))
	if err != nil {
		fmt.Printf("Error starting chaincode: %s", err)
	}

}
