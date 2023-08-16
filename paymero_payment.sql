with pm as (
    (SELECT         
    settlements.transactionid                                                                                       as id 
    ,settlements.createdat::datetime                                                                                as createdat
    ,'settlement'                                                                                                   as "type"
    ,''                                                                                                             as payment_method
    ,lower(settlements."internalstatus")                                                                            as status
    ,settlements.amount                                                                                             as paid_amount
    ,paid_amount::float                                                                                             as source_amount
    ,settlements.currency                                                                                           as sourcecurrency
    ,paid_amount::float                                                                                             as target_amount
    ,settlements.currency                                                                                           as targetcurrency
    ,settlements.paymeromerchantid                                                                                  as merchantid
    ,settlements.accountid                                                                                          as accountid
    from mongodb.settlements
    WHERE left(settlements."merchantid",7) = 'PM-00CR')
        UNION ALL
    (SELECT
    payments.id                                                                                                     as id
    ,payments.createdat::datetimeas                                                                                 as createdat
    ,payments."type"                                                                                                as "type"
    ,CASE WHEN payments.subtype  = 'invoice'                                            THEN 'invoice'
        WHEN payments.subtype = 'direct'                                                THEN 'direct'
        WHEN payments.subtype is null and DATE(createdat) <= '2000-01-01'               THEN 'direct'                  --new subtype was created from this date
        ELSE NULL END                                                                                               as payment_method
    ,payments.status                                                                                                as status
    ,CASE WHEN payment_method = 'direct'                                                THEN payments.sourceamount::float
        WHEN payment_method = 'invoice' AND payments.status IN ('paid', 'credit')       THEN payments.sourceamount::float
        WHEN payments.status  = 'overpaid'                                              THEN GREATEST(payments.overpaidamount::float, payments.paidamount::float)
        WHEN payments.status  = 'underpaid'                                             THEN GREATEST(payments.underpaidamount::float, payments.paidamount::float)
        ELSE 0 END                                                                                                  as paid_amount
    ,payments.sourceAmount::float                                                                                   as source_amount
    ,payments.sourcecurrency                                                                                        as sourcecurrency
    ,payments.targetamount::float                                                                                   as target_amount
    ,payments.targetcurrency                                                                                        as targetcurrency
    ,payments.merchantid                                                                                            as merchantid
    ,payments.accountid                                                                                             as accountid
    FROM mongodb.payments
    WHERE left(payments.merchantid,7) = 'PM-00CR')
    )
--aggregation of trx table to ensure 1:1 relation between trx & payments tables
,tx as (
    select
    transactions.paymentid
    ,count(transactions.txhash)                 as txhash_count
    ,listagg(transactions.txhash::text,', ')    as txhash
    ,listagg(dest.address::text,', ')           as wallet_address
    ,CASE WHEN lower(addresses.ExternalID) LIKE '%topup%' THEN 'Topup'
        WHEN lower(addresses."type") LIKE '%topup%' THEN 'Topup'
        ELSE null
        END as topup_mark
    FROM mongodb.transactions
    LEFT JOIN mongodb.addresses                                 ON Transactions.AddressID = Addresses.ID
    LEFT JOIN mongodb.transactions__details__destinations dest  ON dest._sdc_source_key__id = transactions._id AND dest._sdc_level_0_id = transactions.__v
    WHERE state = 'confirmed'                        -- only completed trx
    AND paymentid is not NULL                       -- filtering out system issues
    group by paymentid, topup_mark
    )
SELECT 
pm.id                                                                                                       as paymentid
,pm.createdat                                                                                               as createdat
,lower(coalesce(topup_mark, pm."type"::varchar))                                                            as transaction_type
,pm.payment_method                                                                                          as sub_type
,pm.status                                                                                                  as status
,pm.paid_amount                                                                                             as volume
,pm.sourcecurrency                                                                                          as volume_currency
,paid_amount::float*
    (SELECT rate
       FROM   reports.fx_rates_mv fx
       WHERE  targetcurrency = sourcecurrency
       AND    basecurrency = 'GBP'
       AND    date_trunc('day',pm.createdat) = fx.dateday AND rawinverseamount IS NOT NULL)                 as volume_gbp
,source_amount::float*
    (SELECT rate
       FROM   reports.fx_rates_mv fx
       WHERE  targetcurrency = sourcecurrency
       AND    basecurrency = 'GBP'
       AND    date_trunc('day',pm.createdat) = fx.dateday AND rawinverseamount IS NOT NULL)                 as deposit_amount_gbp
,source_amount                                                                                              as deposit_amount
,sourcecurrency                                                                                             as deposit_currency
,target_amount::float*
    (SELECT rawinverseamount
       FROM   reports.fx_rates_mv fx
       WHERE  targetcurrency = sourcecurrency
       AND    basecurrency = 'GBP'
       AND    date_trunc('day',pm.createdat) = fx.dateday AND rawinverseamount IS NOT NULL)
    end                                                                                                     as exit_amount_gbp
,target_amount                                                                                              as exit_amount
,pm.targetcurrency                                                                                          as exit_currency
,pm.merchantid                                                                                              as merchantid
,CASE WHEN cn.registered_company_name is null THEN me.merchantname ELSE cn.registered_company_name END      as client
,pe.entityname                                                                                              as legal_entity
,pmr.vendor                                                                                                 as vendor
,tx.txhash                                                                                                  as txhash
,tx.txhash_count                                                                                            as txhash_count
,tx.wallet_address                                                                                          as wallet_address
,pm.accountid                                                                                               as accountid
FROM pm
LEFT JOIN tx ON pm.id = tx.paymentid
LEFT JOIN dynamodb."prd-merchants" me          ON me.id = pm.merchantid
LEFT JOIN dynamodb."prd-merchants__routes" pmr ON me.id = pmr._sdc_source_key_id
LEFT JOIN gsheets.paymeroentites pe            ON pe.entityid = me.issuerentityid
LEFT JOIN gsheets.clientnames cn               ON me.merchantname = cn.manual_name AND sourcename = 'prd-merchants' --this standardises client names with what we have in TS
WHERE ((lower(client) NOT LIKE '%demo%') OR client is NULL)
