package query

import (
	"fmt"
	"strings"
)

const PgInsertQueryTemplate = `INSERT INTO %[1]s(%[2]s) VALUES (%[3]s)`

// POSTGRES ver15
const Pg15MergeQueryWithoutUpdatedTemplate = `MERGE INTO %[1]s a 
USING (SELECT @S_KEY AS S_KEY, @S_MERCHANT_CHANNEL AS S_MERCHANT_CHANNEL, @S_PAYGATE AS S_PAYGATE, @N_PARTNER_ID::numeric AS N_PARTNER_ID, @S_PARTNER_NAME AS S_PARTNER_NAME, @S_TRANS_TYPE AS S_TRANS_TYPE, @S_RESPONSE_CODE AS S_RESPONSE_CODE, @S_TRANSACTION_ID AS S_TRANSACTION_ID, @S_ORIGINAL_ID AS S_ORIGINAL_ID, @S_MSP_TXN_ID AS S_MSP_TXN_ID, @S_INVOICE_ID AS S_INVOICE_ID, @S_MERCHANT_ID AS S_MERCHANT_ID, @S_MERCHANT_TRANS_REF AS S_MERCHANT_TRANS_REF, @S_ORDER_REF AS S_ORDER_REF, @N_TRANS_AMOUNT::numeric AS N_TRANS_AMOUNT, @S_CURRENCY AS S_CURRENCY, @S_TRANS_STATE AS S_TRANS_STATE, @D_TRANSACTION::timestamp AS D_TRANSACTION, @D_UPDATE_BNPL_18::timestamp AS D_UPDATE_BNPL_18, @S_CARD_TYPE AS S_CARD_TYPE, @S_CARD_NUMBER AS S_CARD_NUMBER, @N_ORIGIN_AMOUNT::numeric AS N_ORIGIN_AMOUNT, @D_ORIGIN_DATE::timestamp AS D_ORIGIN_DATE, @S_INVOICE_STATE AS S_INVOICE_STATE, @S_PROVIDER_MESSAGE AS S_PROVIDER_MESSAGE, @J_DATA AS J_DATA, @S_CONTRACT_TYPE AS S_CONTRACT_TYPE, @S_BANK_TRANS_ID AS S_BANK_TRANS_ID, @N_PROMOTION::numeric AS N_PROMOTION, @S_QR_CHANNEL AS S_QR_CHANNEL, @S_PROMOTION_CODE AS S_PROMOTION_CODE, @S_PROMOTION_NAME AS S_PROMOTION_NAME, @S_RAW_DATA AS S_RAW_DATA, @S_REFERRAL_RAW AS S_REFERRAL_RAW) AS b ON a.S_KEY = b.S_KEY and a.D_TRANSACTION = b.D_TRANSACTION WHEN matched THEN UPDATE SET S_MERCHANT_CHANNEL = b.S_MERCHANT_CHANNEL, S_PAYGATE = b.S_PAYGATE, N_PARTNER_ID = b.N_PARTNER_ID, S_PARTNER_NAME = b.S_PARTNER_NAME, S_TRANS_TYPE = b.S_TRANS_TYPE, S_RESPONSE_CODE = b.S_RESPONSE_CODE, S_TRANSACTION_ID = b.S_TRANSACTION_ID, S_ORIGINAL_ID = b.S_ORIGINAL_ID, S_MSP_TXN_ID = b.S_MSP_TXN_ID, S_INVOICE_ID = b.S_INVOICE_ID, S_MERCHANT_ID = b.S_MERCHANT_ID, S_MERCHANT_TRANS_REF = b.S_MERCHANT_TRANS_REF, S_ORDER_REF = b.S_ORDER_REF, N_TRANS_AMOUNT = b.N_TRANS_AMOUNT, S_CURRENCY = b.S_CURRENCY, S_TRANS_STATE = b.S_TRANS_STATE, D_TRANSACTION = b.D_TRANSACTION, D_UPDATE_BNPL_18 = b.D_UPDATE_BNPL_18, S_CARD_TYPE = b.S_CARD_TYPE, S_CARD_NUMBER = b.S_CARD_NUMBER, N_ORIGIN_AMOUNT = b.N_ORIGIN_AMOUNT, D_ORIGIN_DATE = b.D_ORIGIN_DATE, S_INVOICE_STATE = b.S_INVOICE_STATE, S_PROVIDER_MESSAGE = b.S_PROVIDER_MESSAGE, J_DATA = b.J_DATA, S_CONTRACT_TYPE = b.S_CONTRACT_TYPE, S_BANK_TRANS_ID = b.S_BANK_TRANS_ID, N_PROMOTION = b.N_PROMOTION, S_QR_CHANNEL = b.S_QR_CHANNEL, S_PROMOTION_CODE = b.S_PROMOTION_CODE, S_PROMOTION_NAME = b.S_PROMOTION_NAME, S_RAW_DATA = b.S_RAW_DATA, S_REFERRAL_RAW = b.S_REFERRAL_RAW WHEN NOT matched THEN INSERT (S_KEY, S_MERCHANT_CHANNEL, S_PAYGATE, N_PARTNER_ID, S_PARTNER_NAME, S_TRANS_TYPE, S_RESPONSE_CODE, S_TRANSACTION_ID, S_ORIGINAL_ID, S_MSP_TXN_ID, S_INVOICE_ID, S_MERCHANT_ID, S_MERCHANT_TRANS_REF, S_ORDER_REF, N_TRANS_AMOUNT, S_CURRENCY, S_TRANS_STATE, D_TRANSACTION, D_UPDATE_BNPL_18, S_CARD_TYPE, S_CARD_NUMBER, N_ORIGIN_AMOUNT, D_ORIGIN_DATE, S_INVOICE_STATE, S_PROVIDER_MESSAGE, J_DATA, S_CONTRACT_TYPE, S_BANK_TRANS_ID, N_PROMOTION, S_QR_CHANNEL, S_PROMOTION_CODE, S_PROMOTION_NAME, S_RAW_DATA, S_REFERRAL_RAW) VALUES (b.S_KEY, b.S_MERCHANT_CHANNEL, b.S_PAYGATE, b.N_PARTNER_ID, b.S_PARTNER_NAME, b.S_TRANS_TYPE, b.S_RESPONSE_CODE, b.S_TRANSACTION_ID, b.S_ORIGINAL_ID, b.S_MSP_TXN_ID, b.S_INVOICE_ID, b.S_MERCHANT_ID, b.S_MERCHANT_TRANS_REF, b.S_ORDER_REF, b.N_TRANS_AMOUNT, b.S_CURRENCY, b.S_TRANS_STATE, b.D_TRANSACTION, b.D_UPDATE_BNPL_18, b.S_CARD_TYPE, b.S_CARD_NUMBER, b.N_ORIGIN_AMOUNT, b.D_ORIGIN_DATE, b.S_INVOICE_STATE, b.S_PROVIDER_MESSAGE, b.J_DATA, b.S_CONTRACT_TYPE, b.S_BANK_TRANS_ID, b.N_PROMOTION, b.S_QR_CHANNEL, b.S_PROMOTION_CODE, b.S_PROMOTION_NAME, b.S_RAW_DATA, b.S_REFERRAL_RAW)`

func GenInsertQueryFromMap(table string, val map[string]any) string {
	var columns []string
	var values []string
	for k, v := range val {
		columns = append(columns, k)
		switch v.(type) {
		case string:
			values = append(values, fmt.Sprintf("'%s'", v))
		default:
			values = append(values, fmt.Sprintf("%v", v))
		}
	}
	return fmt.Sprintf(PgInsertQueryTemplate, table, strings.Join(columns, ","), strings.Join(values, ","))
}

func UpdateQueryFormMapWithOutUpdated(table string, val map[string]any) string {
	var columns []string
	var values []string
	for k, v := range val {
		columns = append(columns, k)
		switch v.(type) {
		case string:
			values = append(values, fmt.Sprintf("'%s'", v))
		default:
			values = append(values, fmt.Sprintf("%v", v))
		}
	}
	return fmt.Sprintf(Pg15MergeQueryWithoutUpdatedTemplate, table, strings.Join(columns, ","), strings.Join(values, ","))

}
