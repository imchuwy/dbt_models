with pl as (
    SELECT 
    transactionid
    ,transactiontype
    ,amount
    ,decode(debitcredit,'CREDIT',currency)                              as credit_currency
    ,decode(debitcredit,'DEBIT',currency)                               as debit_currency
    ,pbt.createdat::datetime
    ,debitcredit
--following is for manual adjustments
    ,coalesce(pbt.paymeromerchantid,split_part(pbt.remarks, '#', 2))    as merchantid
    ,case when remarks like '%#%' then 'adjustment' else null END       as PL_type
    ,case when pl_type = 'adjustment' then (
        select merchantname from paymerodynamo."prd-merchants" where "prd-merchants".id = split_part(pbt.remarks, '#', 2))
        else null
        end as merchantname
    ,split_part(pbt.remarks, '#', 5) paymentid
    ,split_part(pbt.remarks, '#', 3) subtype
    ,pbt.accountid
-- GBP conversion rates for reporting   
    ,amount::float*
        (SELECT rawinverseamount
        FROM   reports.fx_rates_mv fx
        WHERE  targetcurrency = pbt.currency
        AND    basecurrency = 'GBP'
        AND    date_trunc('day',pbt.createdat) = fx.dateday AND rawinverseamount IS NOT NULL        as Amount_GBP
    ,CASE WHEN upper(transactiontype) = 'FEE'                   THEN Amount_GBP::float ELSE 0 END   as fee_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'NETWORK_FEE'           THEN Amount_GBP::float ELSE 0 END   as network_fee_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'NETWORK_FEE_COVERAGE'  THEN Amount_GBP::float ELSE 0 END   as network_fee_coverage_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'NETWORK_INTERNAL'      THEN Amount_GBP::float ELSE 0 END   as network_internal_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'FEE_REFUND'            THEN Amount_GBP::float ELSE 0 END   as fee_refund_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'NETWORK_FEE_REFUND'    THEN Amount_GBP::float ELSE 0 END   as network_fee_refund_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'TRANSFER_EXTERNAL'     THEN Amount_GBP::float ELSE 0 END   as transfer_external_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'TOPUP_EXTERNAL'        THEN Amount_GBP::float ELSE 0 END   as topup_external_amount_gbp
    ,CASE WHEN upper(transactiontype) = 'FX' AND upper(debitcredit) = 'CREDIT'  THEN Amount_GBP::float 
        WHEN upper(transactiontype) = 'FX' AND upper(debitcredit) = 'DEBIT'     THEN (Amount_GBP::float) * -1
        ELSE 0 END                                                                                  as FX_amount_gbp
    ,case when upper(transactiontype) in ('FEE','NETWORK_FEE','NETWORK_FEE_COVERAGE') then amount::float
        when upper(transactiontype) in ('FEE_REFUND','NETWORK_FEE_REFUND','TRANSFER_EXTERNAL') then -amount::Float
        when upper(transactiontype) = 'FX' and debitcredit = 'CREDIT' then amount::Float
        when upper(transactiontype) = 'FX' and debitcredit = 'DEBIT' then -amount::float
        else 0 end                                                                                  as gross_revenue_local
    ,case when upper(transactiontype) = 'NETWORK_INTERNAL' then amount::float else 0 end            as cost_local
    ,CASE WHEN upper(debitcredit) = 'CREDIT'    then 1 Else 0 END                                   as credit_entries
    ,CASE WHEN upper(debitcredit) = 'DEBIT'     then 1 Else 0 END                                   as debit_entries
    FROM paymero.paymerobalancetransactions pbt
    WHERE left(merchantid,2) <> 'SB'
        AND (pbt.paymentmethod in ('CRYPTO_DEPOSIT','CRYPTO_PAYMENT','CRYPTO','CRYPTO_PAYOUT') or (pbt.paymentmethod is null and split_part(pbt.remarks, '#', 3) is not null))  --We only want crypto ledger entries and manual entries that are refunds
        AND pbt.transactionid not in ('bdc4b159-9ee7-4b43-ae55-0fe8ef6676cb','f069db12-f8d3-4219-85d9-b6776dae1465','6fd59dff-85a2-42c8-b10d-23f61f8274a0','acc6b3c1-33db-4b81-a102-b377bf6fd854','ce695fde-ab02-45c7-bfac-c2ab1a330460','16f621dc-ab70-48bf-aa31-cc9e2adbd335','a8dd5f75-702f-459e-8dd8-ea0b7f4b53f9','b9fafd9e-d5ab-456e-873f-570d67d589be','e8f730e0-14c8-48ca-a8dd-49182be632e3')     -- filtering out bugs
        AND not (upper(pbt.transactiontype) in ('NETWORK_FEE','NETWORK_FEE_COVERAGE','NETWORK_INTERNAL') and pbt.createdat < '2022/09/06')                                      -- new rule for revenue tracking since '2022/09/06'
)
,pl_pm_tx as (
    SELECT
     coalesce(pl.transactionid, pm_tx.paymentid, pl.paymentid) as paymentid
    ,pm_tx.createdat                                    as createdat
    ,MIN(pl.createdat::datetime)                        as ledger_createdat
    ,SUM(fee_amount_gbp::float)                         as fee_amount_gbp
    ,SUM(network_fee_amount_gbp::float)                 as network_fee_amount_gbp
    ,SUM(network_fee_coverage_amount_gbp::float)        as network_fee_coverage_amount_gbp
    ,SUM(network_internal_amount_gbp::float)            as network_internal_amount_gbp
    ,SUM(fee_refund_amount_gbp::float)                  as fee_refund_amount_gbp
    ,SUM(network_fee_refund_amount_gbp::float)          as network_fee_refund_amount_gbp
    ,SUM(topup_external_amount_gbp::float)              as topup_external_amount_gbp
    ,SUM(transfer_external_amount_gbp::float)           as transfer_external_amount_gbp
    ,SUM(FX_amount_gbp::float)                          as FX_amount_gbp
    ,sum(gross_revenue_local::float)                    as gross_revenue_local
    ,sum(cost_local::float)                             as cost_local
    ,SUM(credit_entries::float)                         as credit_entries
    ,SUM(debit_entries::float)                          as debit_entries
    ,max(credit_currency)                               as credit_currency
    ,max(debit_currency)                                as debit_currency
    ,pm_tx.accountid
    ,coalesce(max(PL_type), max(pm_tx.transaction_type)) as transaction_type
    ,coalesce(pm_tx.sub_type, pl.subtype)               as sub_type
    ,pm_tx.status
    ,pm_tx.deposit_amount_gbp
    ,pm_tx.deposit_amount
    ,pm_tx.deposit_currency
    ,pm_tx.exit_amount_gbp
    ,pm_tx.exit_amount
    ,pm_tx.exit_currency
    ,pm_tx.txhash_count
    ,pm_tx.txhash
    ,pm_tx.wallet_address
    ,pm_tx.merchantid
    ,coalesce(pm_tx.client,pl.merchantname) client
    ,pm_tx.vendor
    ,pm_tx.legal_entity
    FROM maint.paymero_payment_table pm_tx
    FULL OUTER JOIN pl ON pm_tx.paymentid = pl.transactionid
    GROUP BY pm_tx.paymentid, transactionid, pl.paymentid, pm_tx.createdat, pm_tx.accountid, transaction_type, sub_type, pl.subtype, status
        ,deposit_amount_gbp, deposit_amount, deposit_currency, exit_amount_gbp, exit_amount, exit_currency, txhash_count, txhash, wallet_address, pm_tx.merchantid, client, pl.merchantname, vendor, legal_entity
)
select 
paymentid                                               as transactionid
,coalesce(createdat,ledger_createdat)                   as "datetime"
,ledger_createdat
,(fee_amount_gbp::float + network_fee_amount_gbp::float + network_fee_coverage_amount_gbp::float + topup_external_amount_gbp::float
        - fee_refund_amount_gbp::float - network_fee_refund_amount_gbp::float - transfer_external_amount_gbp::float) as gross_revenue_in_gbp
,network_internal_amount_gbp                            as cost_in_gbp
,(gross_revenue_in_gbp::float - cost_in_gbp::float)     as net_revenue_in_gbp
,fee_amount_gbp
,network_fee_amount_gbp
,network_fee_coverage_amount_gbp
,network_internal_amount_gbp
,fee_refund_amount_gbp
,network_fee_refund_amount_gbp
,FX_amount_gbp
,gross_revenue_local
,cost_local
,credit_currency    as currency
,debit_currency
,credit_entries
,debit_entries
,pl_pm_tx.accountid
,transaction_type
,sub_type
,status
,deposit_amount_gbp
,deposit_amount
,deposit_currency
,exit_amount_gbp
,exit_amount
,exit_currency
,txhash_count
,txhash
,wallet_address
,client
,vendor                  
,legal_entity
from pl_pm_tx