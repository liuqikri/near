package bal

import (
	"container/list"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"payment02/src/util"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

//账户信息记录对象
type TxInstr struct {
	INSTRID   string //交易指示ID
	BKCODE    string // 转出银行号
	ACTNOFROM string // 转出账号
	CLRBKCDE  string //转入银行号
	ACTNOTO   string // 转入账号
	TXAMT     string //交易金额
	CURCDE    string //货币码
	TXNDAT    string //交易日期
	TXNTIME   string //交易时间
	COMPST    string //交易完成状态
	RTCDE     string //交易状态代码
}

//NOSTRO余额表
type TdNoStroBal struct {
	ACTID     string // 往来账户ID
	BKCODE    string // 银行号
	CLRBKCDE  string //资金清算银行号
	CURCDE    string //货币码
	NOSTROBAL string //Nostro余额
}

//NOSTRO余额表录入(初始化)
func InsertTdNoStroBal(stub shim.ChaincodeStubInterface, data TdNoStroBal) ([]byte, error) {
	//往账户表插入数据
	ok, err := stub.InsertRow(util.TdNoStroBal, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: data.ACTID}},      // 往来账户ID
			&shim.Column{Value: &shim.Column_String_{String_: data.BKCODE}},     // 银行号
			&shim.Column{Value: &shim.Column_String_{String_: data.CLRBKCDE}},   //资金清算银行号
			&shim.Column{Value: &shim.Column_String_{String_: data.CURCDE}},     // 货币码
			&shim.Column{Value: &shim.Column_String_{String_: data.NOSTROBAL}}}, //Nostro余额
	})

	if !ok && err == nil {
		return nil, errors.New("Table TdNoStroBal insert failed.")
	}
	//更新或者插入table_count表
	totalNumber, err := util.UpdateTableCount(stub, util.TdNoStroBal)
	if totalNumber == 0 || err != nil {
		stub.DeleteRow(util.TdNoStroBal, []shim.Column{shim.Column{Value: &shim.Column_String_{String_: data.ACTID}}})
		return nil, errors.New("InsertTdNoStroBal Table_Count insert failed")
	}
	//把获取的总数插入行号表中当主键
	err = util.UpdateRowNoTable(stub, util.TdNoStroBal_Rownum, data.ACTID, totalNumber)
	if err != nil {
		stub.DeleteRow(util.TdNoStroBal, []shim.Column{shim.Column{Value: &shim.Column_String_{String_: data.ACTID}}})
		var columns []shim.Column
		col := shim.Column{Value: &shim.Column_String_{String_: util.TdNoStroBal}}
		columns = append(columns, col)
		row, _ := stub.GetRow(util.Table_Count, columns) //row是否为空
		if len(row.Columns) == 1 {
			stub.DeleteRow(util.Table_Count, []shim.Column{shim.Column{Value: &shim.Column_String_{String_: util.TdNoStroBal}}})
		} else {
			stub.ReplaceRow(util.Table_Count, shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: util.TdNoStroBal}}, //表名
					&shim.Column{Value: &shim.Column_Int64{Int64: totalNumber - 1}}},     //总数
			})
		}
		return nil, errors.New("InsertTdNoStroBal insert TdNoStroBal_Rownum failed")
	}
	log.Println("InsertTdNoStroBal success.")
	return nil, nil
}

//向交易指示表中插入交易信息
func InsertTxInstr(stub shim.ChaincodeStubInterface, transObject *TxInstr) error {
	defer util.End(util.Begin("InsertTxInstr"))
	//插入交易指示表中插入交易表
	ok, err := stub.InsertRow(util.TxInstr, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: transObject.INSTRID}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.BKCODE}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.ACTNOFROM}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.CLRBKCDE}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.ACTNOTO}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.TXAMT}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.CURCDE}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.TXNDAT}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.TXNTIME}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.COMPST}},
			&shim.Column{Value: &shim.Column_String_{String_: transObject.RTCDE}}},
	})
	if !ok {
		log.Println("InsertRow transObject error..")
		return errors.New("TxInstr insertion failed.")
	}
	//更新table_count表
	totalNo, err := util.UpdateTableCount(stub, util.TxInstr)
	if totalNo == 0 || err != nil {
		// 回滚交易表记录
		stub.DeleteRow(util.TxInstr, []shim.Column{shim.Column{Value: &shim.Column_String_{String_: transObject.INSTRID}}})

		log.Println("update table_count error..")
		return errors.New("Total_Count update failed")
	}
	//更新行号表
	err = util.UpdateRowNoTable(stub, util.TxInstr_Rownum, transObject.INSTRID, totalNo)
	if err != nil {
		// 回滚交易表
		stub.DeleteRow(util.TxInstr, []shim.Column{shim.Column{Value: &shim.Column_String_{String_: transObject.INSTRID}}})
		// 回滚table_count表
		var columns []shim.Column
		col := shim.Column{Value: &shim.Column_String_{String_: util.TxInstr}}
		columns = append(columns, col)
		row, _ := stub.GetRow(util.Table_Count, columns) //row是否为空
		if len(row.Columns) == 1 {
			stub.DeleteRow(util.Table_Count, []shim.Column{shim.Column{Value: &shim.Column_String_{String_: util.TxInstr}}})
		} else {
			stub.ReplaceRow(util.Table_Count, shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: util.TxInstr}}, //表名
					&shim.Column{Value: &shim.Column_Int64{Int64: totalNo - 1}}},     //总数
			})
		}

		log.Println("update TxInstr_Rownum error..")
		return errors.New("TxInstr_Rownum insert failed")
	}
	log.Println("TxInstr_Rownum insert sucess!")

	return nil
}

//更新交易表数据

func UpdateTxInstr(stub shim.ChaincodeStubInterface, data *TxInstr) error {
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: data.INSTRID}}
	columns = append(columns, col)
	row, _ := stub.GetRow(util.TxInstr, columns)

	if len(row.Columns) != 0 {

		ok, err := stub.ReplaceRow(util.TxInstr, shim.Row{
			Columns: []*shim.Column{

				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[0].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[1].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[2].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[3].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[4].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[5].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[6].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[7].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[8].GetString_()}},
				&shim.Column{Value: &shim.Column_String_{String_: data.COMPST}}, // 交易完成状态
				&shim.Column{Value: &shim.Column_String_{String_: data.RTCDE}}}, // 交易完成码

		})
		if !ok && err == nil {
			return errors.New("method UpdateTxInstr  ReplaceRow failed.")
		}
	}
	log.Println("UpdateTxInstr success.")
	return nil
}

//更新NOSTRO余额表信息
func UpdateTdNoStroBal(stub shim.ChaincodeStubInterface, data *TdNoStroBal) error {
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: data.ACTID}}
	columns = append(columns, col)
	row, _ := stub.GetRow(util.TdNoStroBal, columns)
	if len(row.Columns) != 0 {
		ok, err := stub.ReplaceRow(util.TdNoStroBal, shim.Row{
			Columns: []*shim.Column{

				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[0].GetString_()}}, // 往来账户ID
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[1].GetString_()}}, // 银行号
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[2].GetString_()}}, //资金清算银行号
				&shim.Column{Value: &shim.Column_String_{String_: row.Columns[3].GetString_()}}, //货币码
				&shim.Column{Value: &shim.Column_String_{String_: data.NOSTROBAL}}},             //Nostro余额

		})
		if !ok && err == nil {
			return errors.New("method updateTdNoStroBal TdNoStroBal ReplaceRow failed.")
		}
	}
	log.Println("UpdateAccount success.")
	return nil
}

//根据主键查询nos记录
func QueryTdNoStroBalRecordByKey(stub shim.ChaincodeStubInterface, ACTID string) ([]byte, error) {
	defer util.End(util.Begin("QueryTdCstActBalRecordByKey"))
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: ACTID}}
	columns = append(columns, col)
	row, _ := stub.GetRow(util.TdNoStroBal, columns)
	if len(row.Columns) == 0 { //row是否为空
		var errorMsg = "Table TdNoStroBal: specified record doesn't exist,TdNoStroBal id = " + ACTID
		log.Println(errorMsg)
		return nil, errors.New(errorMsg)
	} else {

		jsonResp := `{"ACTID":"` + row.Columns[0].GetString_() + `","BKCODE":"` + row.Columns[1].GetString_() +
			`","CLRBKCDE":"` + row.Columns[2].GetString_() + `","CURCDE":"` + row.Columns[3].GetString_() +
			`","NOSTROBAL":"` + row.Columns[4].GetString_() + `"}`

		log.Println("jsonResp:" + jsonResp)
		return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"OK","errMsg":"查询成功","data":` + jsonResp + `}`))), nil
	}
}

//根据主键查询NOSTROBAL账户余额
func QueryTdNoStroBalBalanceByKey(stub shim.ChaincodeStubInterface, ACTID string) string {
	defer util.End(util.Begin("QueryTdNoStroBalBalanceByKey"))

	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: ACTID}}
	columns = append(columns, col)
	row, _ := stub.GetRow(util.TdNoStroBal, columns)
	if len(row.Columns) == 0 { //row是否为空
		var errorMsg = "Table TdNoStroBal: specified record doesn't exist,TdNoStroBal id = " + ACTID
		log.Println(errorMsg)
		panic(errorMsg)
	} else {
		NOSTROBAL := row.Columns[4].GetString_()
		log.Println("NOSTROBAL is = " + NOSTROBAL)
		fmt.Println("NOSTROBAL is = " + NOSTROBAL)
		return NOSTROBAL
	}
}

//查询单个人的所有交易数据 flag:in/out
func SelectSimpleAllTxInstr(stub shim.ChaincodeStubInterface, Account string, curPage int, flag string) ([]byte, error) {
	defer util.End(util.Begin("SelectSimpleAllTxInstr"))

	//查询所有的数据
	listResult := list.New()
	var rowtwo []shim.Row
	var n *list.Element
	//获得所有的满足条件的交易表所有的行

	rowIds := util.QueryTableLinesInt(stub, "TxInstr") //获取TxInstr行号表的的所有主键
	rowIdsAll, _ := strconv.Atoi(rowIds)
	fmt.Println("stq rowIdsAll  " + rowIds)
	fmt.Println(rowIdsAll)
	for j := 1; j <= rowIdsAll; j++ {
		//查询行号表
		TxInstrId := QueryIdByKey(stub, strconv.Itoa(j)) //  获取行号表里面交易表的Id
		fmt.Println("stq TxInstrId  " + TxInstrId)
		var columns []shim.Column
		col := shim.Column{Value: &shim.Column_String_{String_: TxInstrId}}
		columns = append(columns, col)
		row, _ := stub.GetRow(util.TxInstr, columns)
		listResult.PushBack(row) //获取全部的数据百度往list里面添加数据
	}

	//刷选 满足条件的加入到list
	for it := listResult.Front(); it != nil; it = n {
		row := it.Value.(shim.Row)
		n = it.Next()
		if flag == "out" {
			if row.Columns[2].GetString_() == Account { //条件查询转出

				rowtwo = append(rowtwo, it.Value.(shim.Row))
			} else {
				listResult.Remove(it)

			}
		} else {
			if row.Columns[4].GetString_() == Account { //条件查询转入

				rowtwo = append(rowtwo, it.Value.(shim.Row))
			} else {
				listResult.Remove(it)
			}

		}
	}
	//遍历出所有的数据
	var strItemList string
	strItemList = `"itemList":[`
	totalNumber := listResult.Len() //刷选完后的总条数
	totalPage := 0                  //总页数
	if totalNumber%10 == 0 {
		//如果总页数是10的倍数，则总页数=
		totalPage = totalNumber / 10
	} else {
		//如果总页数不是10的倍数，则总页数=
		totalPage = totalNumber/10 + 1
	}
	if curPage > totalPage-1 {
		return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"fail","errMsg":"当前页大于总页数"}`))), errors.New("找不到数据")
	}
	nStart := curPage * 10
	nEnd := nStart + 10
	if nEnd > listResult.Len() {
		nEnd = listResult.Len()
	}
	//遍历当前页所有的行
	tempCount := 0
	for i := nStart; i < nEnd; i++ {
		row := rowtwo[i]
		strRow := `{"INSTRID":"` + row.Columns[0].GetString_() + `","BKCODE":"` + row.Columns[1].GetString_() +
			`","ACTNOFROM":"` + row.Columns[2].GetString_() + `","CLRBKCDE":"` + row.Columns[3].GetString_() +
			`","ACTNOTO":"` + row.Columns[4].GetString_() + `","TXAMT":"` + row.Columns[5].GetString_() +
			`","CURCDE":"` + row.Columns[6].GetString_() + `","TXNDAT":"` + row.Columns[7].GetString_() + `","TXNTIME":"` + row.Columns[8].GetString_() +
			`","COMPST":"` + row.Columns[9].GetString_() + `","RTCDE":"` + row.Columns[10].GetString_() + `"}`
		if tempCount != len(rowtwo)-1 {
			strRow += ","
		}
		strItemList += strRow
		tempCount++

	}
	strItemList += `]`

	jsonResp := `{"totalpage":"` + strconv.Itoa(totalPage) + `","curpage":"` + strconv.Itoa(curPage) + `",` + strItemList + `}`
	return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"OK","errMsg":"查询成功","data":` + jsonResp + `}`))), nil

}

//查询Blank的所有交易数据 flag:in/out
func SelectBlankAllTxInstr(stub shim.ChaincodeStubInterface, Account string, curPage int, flag string) ([]byte, error) {
	defer util.End(util.Begin("SelectSimpleAllTxInstr"))

	//查询所有的数据
	listResult := list.New()
	var rowtwo []shim.Row
	var n *list.Element
	//获得所有的满足条件的交易表所有的行

	rowIds := util.QueryTableLinesInt(stub, "TxInstr") //获取TxInstr行号表的的所有主键
	rowIdsAll, _ := strconv.Atoi(rowIds)
	fmt.Println("stq rowIdsAll  " + rowIds)
	fmt.Println(rowIdsAll)
	for j := 1; j <= rowIdsAll; j++ {
		//查询行号表
		TxInstrId := QueryIdByKey(stub, strconv.Itoa(j)) //  获取行号表里面交易表的Id
		var columns []shim.Column
		col := shim.Column{Value: &shim.Column_String_{String_: TxInstrId}}
		columns = append(columns, col)
		row, _ := stub.GetRow(util.TxInstr, columns)
		listResult.PushBack(row) //获取全部的数据百度往list里面添加数据
	}

	//刷选 满足条件的加入到list
	for it := listResult.Front(); it != nil; it = n {
		row := it.Value.(shim.Row)
		n = it.Next()
		if flag == "out" {
			if row.Columns[1].GetString_() == Account { //条件查询转出

				rowtwo = append(rowtwo, it.Value.(shim.Row))
			} else {
				listResult.Remove(it)

			}
		} else {
			if row.Columns[3].GetString_() == Account { //条件查询转入

				rowtwo = append(rowtwo, it.Value.(shim.Row))
			} else {
				listResult.Remove(it)
			}

		}
	}
	//遍历出所有的数据
	var strItemList string
	strItemList = `"itemList":[`
	totalNumber := listResult.Len() //刷选完后的总条数
	totalPage := 0                  //总页数
	if totalNumber%10 == 0 {
		//如果总页数是10的倍数，则总页数=
		totalPage = totalNumber / 10
	} else {
		//如果总页数不是10的倍数，则总页数=
		totalPage = totalNumber/10 + 1
	}
	if curPage > totalPage-1 {
		return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"fail","errMsg":"当前页大于总页数"}`))), errors.New("找不到数据")
	}
	nStart := curPage * 10
	nEnd := nStart + 10
	if nEnd > listResult.Len() {
		nEnd = listResult.Len()
	}
	//遍历当前页所有的行
	tempCount := 0
	for i := nStart; i < nEnd; i++ {
		row := rowtwo[i]
		strRow := `{"INSTRID":"` + row.Columns[0].GetString_() + `","BKCODE":"` + row.Columns[1].GetString_() +
			`","ACTNOFROM":"` + row.Columns[2].GetString_() + `","CLRBKCDE":"` + row.Columns[3].GetString_() +
			`","ACTNOTO":"` + row.Columns[4].GetString_() + `","TXAMT":"` + row.Columns[5].GetString_() +
			`","CURCDE":"` + row.Columns[6].GetString_() + `","TXNDAT":"` + row.Columns[7].GetString_() + `","TXNTIME":"` + row.Columns[8].GetString_() +
			`","COMPST":"` + row.Columns[9].GetString_() + `","RTCDE":"` + row.Columns[10].GetString_() + `"}`
		if tempCount != len(rowtwo)-1 {
			strRow += ","
		}
		strItemList += strRow
		tempCount++

	}
	strItemList += `]`

	jsonResp := `{"totalpage":"` + strconv.Itoa(totalPage) + `","curpage":"` + strconv.Itoa(curPage) + `",` + strItemList + `}`
	return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"OK","errMsg":"查询成功","data":` + jsonResp + `}`))), nil

}

//查询满足(新增N)汇入银行的所有的表里的数据没有分页
func QueryTxInstrRecordAll(stub shim.ChaincodeStubInterface, CLRBKCDE string) ([]byte, error) {
	defer util.End(util.Begin("QueryTxInstrRecordAll"))
	//查询所有的数据
	listResult := list.New()
	var rowtwo []shim.Row
	var n *list.Element
	//获得所有的满足条件的交易表所有的行
	rowIds := util.QueryTableLinesInt(stub, "TxInstr") //获取TxInstr行号表的的所有主键
	rowIdsAll, _ := strconv.Atoi(rowIds)
	for j := 1; j <= rowIdsAll; j++ {
		//查询行号表
		TxInstrId := QueryIdByKey(stub, strconv.Itoa(j)) // 获取行号表里面交易表的Id
		var columns []shim.Column
		col := shim.Column{Value: &shim.Column_String_{String_: TxInstrId}}
		columns = append(columns, col)
		row, _ := stub.GetRow(util.TxInstr, columns)
		listResult.PushBack(row) //获取全部的数据百度List:PushBack
	}

	//刷选 满足条件的加入到list
	for it := listResult.Front(); it != nil; it = n {
		row := it.Value.(shim.Row)
		n = it.Next()
		if row.Columns[9].GetString_() == "N" && row.Columns[3].GetString_() == CLRBKCDE { //条件查询

			rowtwo = append(rowtwo, it.Value.(shim.Row))
		} else {
			listResult.Remove(it)
		}
	}
	//遍历出所有的数据
	var strItemList string
	strItemList = `"itemList":[`
	totalNumber := listResult.Len() //刷选完后的总条数
	//遍历当前页所有的行
	tempCount := 0
	for i := 0; i < totalNumber; i++ {

		row := rowtwo[i]
		strRow := `{"INSTRID":"` + row.Columns[0].GetString_() + `","BKCODE":"` + row.Columns[1].GetString_() +
			`","ACTNOFROM":"` + row.Columns[2].GetString_() + `","CLRBKCDE":"` + row.Columns[3].GetString_() +
			`","ACTNOTO":"` + row.Columns[4].GetString_() + `","TXAMT":"` + row.Columns[5].GetString_() +
			`","CURCDE":"` + row.Columns[6].GetString_() + `","TXNDAT":"` + row.Columns[7].GetString_() + `","TXNTIME":"` + row.Columns[8].GetString_() +
			`","COMPST":"` + row.Columns[9].GetString_() + `","RTCDE":"` + row.Columns[10].GetString_() + `"}`

		if tempCount != len(rowtwo)-1 {
			strRow += ","
		}
		strItemList += strRow
		tempCount++

	}
	strItemList += `]`

	jsonResp := strItemList
	return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"OK","errMsg":"查询成功","data":{` + jsonResp + `}}`))), nil

}

//通过附表查询相应表的真正的ID
func QueryIdByKey(stub shim.ChaincodeStubInterface, Id string) string {
	defer util.End(util.Begin("QueryIdByKey"))
	fmt.Println("QueryIdByKey" + Id)
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: Id}}
	columns = append(columns, col)
	row, _ := stub.GetRow(util.TxInstr_Rownum, columns)
	if len(row.Columns) == 0 { //row是否为空
		var errorMsg = "Table TxInstr_Rownum: specified record doesn't exist,TxInstr_Rownum id = " + Id
		log.Println(errorMsg)
		panic(errorMsg)
	} else {
		TxInstrId := row.Columns[1].GetString_()
		log.Println("ACTID is = " + TxInstrId)
		log.Println("TdCstActBa ACTBAL is = " + TxInstrId)
		fmt.Println("QueryIdByKey TxInstrId" + TxInstrId)
		return TxInstrId
	}
}
