package wrapper

import (
	"errors"
	"fmt"
	"log"
	"payment02/src/bal"
	"payment02/src/util"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

//初始化两张表NOSTRO余额表和表客户账户余额表
type InitTableData struct {
	TdNoStroBal []*bal.TdNoStroBal
}

type OutMoneyList struct {
	TdNoStroBalList   *bal.TdNoStroBal //修改后的东西
	MoenyTransferList []*bal.TxInstr
}
type OutMoneyStruct struct {
	MoenyTransfer *bal.TxInstr
}
type NOSTROID struct {
	ACTID string
}
type SimpleAllTransferData struct {
	Account string
	CurPage int
}
type BlankAllTransferDataAdd struct {
	CLRBKCDE string
}

type BlankAllTransferData struct {
	BlankInOrOut string
	CurPage      int
}

//汇款初始化数据(NOSTRO余额表和客户账户余额表)------------------start-----------------------------
//初始化数据
func InitData(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("InitData"))
	defer func() {
		e := recover()
		if e != nil {
			fmt.Println("初始化数据抛出异常:", e)
			log.Println("初始化数据抛出异常:", e)
		}
	}()

	data := new(InitTableData)
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	for j := 0; j < len(data.TdNoStroBal); j++ {
		bal.InsertTdNoStroBal(stub, *data.TdNoStroBal[j])
	}
	log.Println("InitData success.")
	return nil, nil
}

//*****************************查询功能start************************************

//查询NOSTRO余额表的记录（只查一条数据）
func QueryTdNoStroBalRecordByKey(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("QueryTdNoStroBalRecordByKey"))
	data := new(NOSTROID) //
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	return bal.QueryTdNoStroBalRecordByKey(stub, data.ACTID)
}

//查询个人所有的交易记录转出out
func SelectSimpleAllTxInstrOut(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("SelectSimpleAllTxInstrOut"))
	data := new(SimpleAllTransferData) //传入转入的银行账号
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	return bal.SelectSimpleAllTxInstr(stub, data.Account, data.CurPage, "out")
}

//查询个人所有的交易记录装入的in
func SelectSimpleAllTxInstrIn(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("SelectSimpleAllTxInstrIn"))
	data := new(SimpleAllTransferData) //传入转入的银行账号 和当前的页数
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	return bal.SelectSimpleAllTxInstr(stub, data.Account, data.CurPage, "in")
}

//查询所有的银行转出的交易表数据（out）
func SelectBlankAllTxInstrOut(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("SelectSimpleAllTxInstrIn"))
	data := new(BlankAllTransferData) //传入转入的银行账号 和当前的页数
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	return bal.SelectBlankAllTxInstr(stub, data.BlankInOrOut, data.CurPage, "out")
}

//查询所有的银行转出的交易表数据（in）
func SelectBlankAllTxInstrIn(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("SelectSimpleAllTxInstrIn"))
	data := new(BlankAllTransferData) //传入转入的银行账号 和当前的页数
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	return bal.SelectBlankAllTxInstr(stub, data.BlankInOrOut, data.CurPage, "in")
}

//*****************************查询功能end************************************

//------------------------------交易开始start-----------------------
//第一步：首先汇出账户点击提交按钮，插入交易指示表N（TX_INSTR）数据交易完成状态码（COMPST）
func Payment(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("outMoney"))
	defer func() {
		e := recover()
		if e != nil {
			fmt.Println("转出账户:", e)
			log.Println("转出账户:", e)
		}
	}()
	// 解析传入数据
	creditObject := new(OutMoneyStruct)
	err := util.ParseJsonAndDecode(creditObject, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}

	// 发生一笔交易，插入一条数据到（交易指示表）并且在（客户账户余额表）update冻结金额
	if creditObject.MoenyTransfer.COMPST == "N" {

		//往交易表中插入数据
		err = bal.InsertTxInstr(stub, creditObject.MoenyTransfer)
		if err != nil {
			log.Println("Error occurred when performing InsertTxInstr")
			return nil, errors.New("Error occurred when performing InsertTxInstr.")
		}

	} else {
		log.Println("Flag is not N,that's why we do nothing for table points_transation")
	}

	return nil, nil
}

//汇入的银行查询到有（N-new added）的账户 请求银行核心系统处理（后台定时任务查询）
func SelectAllTxInstr(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("SelectAllTxInstr"))
	data := new(BlankAllTransferDataAdd) //传入转入的银行号
	err := util.ParseJsonAndDecode(data, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	return bal.QueryTxInstrRecordAll(stub, data.CLRBKCDE)
}

//得到银行的的核心码，往【交易指示表】插入修改状态后的数据）
func UpdateTransferStatus(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	defer util.End(util.Begin("UpdateTransferStatus"))
	defer func() {
		e := recover()
		if e != nil {
			fmt.Println("修改状态:", e)
			log.Println("修改状态:", e)
		}
	}()
	// 解析传入数据
	creditObject := new(OutMoneyList)
	err := util.ParseJsonAndDecode(creditObject, args)
	if err != nil {
		log.Println("Error occurred when parsing json")
		return nil, errors.New("Error occurred when parsing json.")
	}
	//传进来的应该是一个List 应该是多条数据
	for i := 0; i < len(creditObject.MoenyTransferList); i++ {

		// 往交易表（不管交易是否成功）这里是修改数据
		err = bal.UpdateTxInstr(stub, creditObject.MoenyTransferList[i]) //这里是修改数据主要是为了改变状态
		if err != nil {
			log.Println("Error occurred when performing InsertTxInstr")
			return nil, errors.New("Error occurred when performing InsertTxInstr.")
		}

		// 修改【NOSTRO余额表】的金额
		if creditObject.MoenyTransferList[i].COMPST == "S" {
			//BKCDE+BNKACTNO+CURCDE【NOSTRO余额表】这里是通过内部查询后的*****start
			NostroId := creditObject.MoenyTransferList[i].BKCODE + creditObject.MoenyTransferList[i].CLRBKCDE + creditObject.MoenyTransferList[i].CURCDE
			balance := bal.QueryTdNoStroBalBalanceByKey(stub, NostroId) // get 【NOSTRO余额】
			fmt.Println("balance" + balance)
			balanceInt, _ := strconv.ParseFloat(balance, 64)
			transferMoney := creditObject.MoenyTransferList[i].TXAMT //获取交易额

			transferMoneyInt, _ := strconv.ParseFloat(transferMoney, 64)
			NOSTROBALChange := balanceInt - transferMoneyInt
			creditObject.TdNoStroBalList.ACTID = NostroId                                             //获取ID
			creditObject.TdNoStroBalList.NOSTROBAL = strconv.FormatFloat(NOSTROBALChange, 'f', 2, 64) //
			fmt.Println("NostroId" + NostroId + "NOSTROBALChange" + creditObject.TdNoStroBalList.NOSTROBAL)
			//BKCDE+BNKACTNO+CURCDE【NOSTRO余额表】这里是通过内部查询后的*****End

			//更新【NOSTRO余额表】修改NOSTRO的值
			err = bal.UpdateTdNoStroBal(stub, creditObject.TdNoStroBalList)
			if err != nil {
				log.Println("Error occurred when performing UpdateTdCstActBal")
				return nil, errors.New("Error occurred when performing UpdateTdCstActBal.")
			}

		} else {
			log.Println("Flag is not S,that's why we do nothing for table TX_INSTR")
		}
	}

	return nil, nil
}
