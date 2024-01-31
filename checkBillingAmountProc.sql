CREATE OR REPLACE PROCEDURE sandbox.tests.checkBillingDollarAmount()
RETURNS varchar
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    DECLARE
        incorrectBillingDollarAmount EXCEPTION(-20001, 'Dollar amount is not correct based on billed_hours.credit_amount and billing.credit_cost');
        calculatedDollarAmount number;
        dollarAmount number;
        creditAmount number;
        billedHour number;
        billedHourHumanReadable timestamp;
    BEGIN
        LET c1 CURSOR FOR (select dollar_amount from billing.billed_hours order by hour desc);
        LET c2 CURSOR FOR (select credit_amount from billing.billed_hours order by hour desc);
        LET c3 CURSOR FOR (select hour from billing.billed_hours order by hour desc);
        SET creditCost = (select dollar_cost from billing.credit_cost);
        open c1;
        open c2;
        open c3;
        fetch c1 into dollarAmount;
        fetch c2 into creditAmount;
        fetch c3 into billedHour;
        SET billedHourHumanReadable := TO_TIMESTAMP(billedHour);
        SET calculatedDollarAmount := ($creditCost * creditAmount);
        IF (dollarAmount <> calculatedDollarAmount) THEN
            RAISE incorrectBillingDollarAmount;
        ELSE
        RETURN 'Billed dollar amount of ' || dollarAmount || ' for hour ' || billedHourHumanReadable || ', credit_cost ' || $creditCost || ', and credit_amount ' || creditAmount || ' is correct.';
        END IF;
        EXCEPTION WHEN incorrectBillingDollarAmount THEN
        BEGIN
            INSERT INTO sandbox.tests.testLog VALUES (CURRENT_TIMESTAMP, :SQLERRM);
        END;
        
    END;
$$;

call sandbox.tests.checkBillingDollarAmount();
select * from sandbox.tests.testLog;