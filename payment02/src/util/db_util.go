package util

import (
	"encoding/base64"
	"errors"
	"log"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

const (
	//汇款交易
	TxInstr            = "TxInstr"
	TxInstr_Rownum     = "TxInstr_rownum"
	TdNoStroBal        = "TdNoStroBal"
	TdNoStroBal_Rownum = "TdNoStroBal_rownum"
	Table_Count        = "table_count"
)

//创建表
func CreateTable(stub shim.ChaincodeStubInterface) error {
	//---------------------------------------汇款创建表------------------------------------
	//交易指示表
	err := stub.CreateTable(TxInstr, []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "INSTRID", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "BKCODE", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "ACTNOFROM", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "CLRBKCDE", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "ACTNOTO", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "TXAMT", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "CURCDE", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "TXNDAT", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "TXNTIME", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "COMPST", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "RTCDE", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {

		return errors.New("create table TxInstr is fail")
	}
	//创建行号表(新增)
	err = stub.CreateTable(TxInstr_Rownum, []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "number", Type: shim.ColumnDefinition_INT64, Key: true},
		&shim.ColumnDefinition{Name: "INSTRID", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	//创建失败删除创建的表
	if err != nil {
		stub.DeleteTable(TxInstr)
		return errors.New("create table TxInstr_Rownum is fail")
	}

	//创建NOSTRO余额表
	err = stub.CreateTable(TdNoStroBal, []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "ACTID", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "BKCODE", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "CLRBKCDE", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "CURCDE", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "NOSTROBAL", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {
		stub.DeleteTable(TxInstr)
		stub.DeleteTable(TxInstr_Rownum)
		return errors.New("create table TdNoStroBal is fail")
	}
	//创建账户行号表(新增)
	err = stub.CreateTable(TdNoStroBal_Rownum, []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "number", Type: shim.ColumnDefinition_INT64, Key: true},
		&shim.ColumnDefinition{Name: "ACTID", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {
		stub.DeleteTable(TxInstr)
		stub.DeleteTable(TxInstr_Rownum)
		stub.DeleteTable(TdNoStroBal)
		return errors.New("create table TdNoStroBal_Rownum is fail")
	}

	//创建总数表
	err = stub.CreateTable(Table_Count, []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "tableName", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "totalAccount", Type: shim.ColumnDefinition_INT64, Key: false},
	})
	if err != nil {
		stub.DeleteTable(TxInstr)
		stub.DeleteTable(TxInstr_Rownum)
		stub.DeleteTable(TdNoStroBal)
		stub.DeleteTable(TdNoStroBal_Rownum)
		return errors.New("create table table_count is fail")
	}
	log.Println("Create Table success.")
	return nil
}

// 更新table_count表
func UpdateTableCount(stub shim.ChaincodeStubInterface, tableName string) (int64, error) {
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: tableName}}
	columns = append(columns, col)
	row, _ := stub.GetRow(Table_Count, columns) //row是否为空
	i := 0
	var totalNumber int64
	totalNumber = int64(i)
	if len(row.Columns) == 0 {
		//若没有数据，则插入总数表一条记录
		totalNumber = 1
		ok, err := stub.InsertRow(Table_Count, shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: tableName}}, //表名
				&shim.Column{Value: &shim.Column_Int64{Int64: totalNumber}}},  //总数
		})
		if !ok {
			return 0, err
		}
	} else {
		totalNumber = row.Columns[1].GetInt64()
		//若有数据，则更新总数
		totalNumber = totalNumber + 1
		ok, err := stub.ReplaceRow(Table_Count, shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: tableName}}, //表名
				&shim.Column{Value: &shim.Column_Int64{Int64: totalNumber}}},  //总数
		})
		if !ok {
			return 0, err
		}
	}
	log.Println("UpdateTableCount success.")
	return totalNumber, nil
}

// 更新行号表
func UpdateRowNoTable(stub shim.ChaincodeStubInterface, rowNumTable, key string, rowNum int64) error {
	ok, err := stub.InsertRow(rowNumTable, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_Int64{Int64: rowNum}},   //行号
			&shim.Column{Value: &shim.Column_String_{String_: key}}}, //数据表主键
	})
	if !ok {
		return err
	}
	log.Println("UpdateRowNoTable success.")
	return nil
}

// 查询表中总行数
func QueryTableLines(stub shim.ChaincodeStubInterface, tableName string) ([]byte, error) {
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: tableName}}
	columns = append(columns, col)
	row, _ := stub.GetRow(Table_Count, columns) //row是否为空
	if len(row.Columns) == 0 {
		return nil, errors.New("Table Table_Count tableName : " + tableName + " not found.")
	} else {
		totalNumber := row.Columns[1].GetInt64()
		jsonResp := `{"totalCount":"` + strconv.FormatInt(totalNumber, 10) + `"}`
		log.Println("jsonResp:" + jsonResp)
		return []byte(base64.StdEncoding.EncodeToString([]byte(`{"status":"OK","errMsg":"查询成功","data":` + jsonResp + `}`))), nil
	}
}

// 查询表中总行数
func QueryTableLinesInt(stub shim.ChaincodeStubInterface, tableName string) string {
	var columns []shim.Column
	col := shim.Column{Value: &shim.Column_String_{String_: tableName}}
	columns = append(columns, col)
	row, _ := stub.GetRow(Table_Count, columns) //row是否为空
	if len(row.Columns) == 0 {
		var errorMsg = "Table TxInstr_Rownum: no data "
		log.Println(errorMsg)
		panic(errorMsg)
	} else {
		totalNumber := row.Columns[1].GetInt64()
		jsonResp := strconv.FormatInt(totalNumber, 10)
		log.Println("jsonResp:" + jsonResp)
		return jsonResp
	}
}

//-----------------------------------end--------------------------------------------------------------------
