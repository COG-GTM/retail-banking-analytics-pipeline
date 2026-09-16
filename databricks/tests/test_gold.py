from retail_banking.parity import compare_gold_outputs


def test_gold_parity(gold_actual, gold_expected):
    report = compare_gold_outputs(gold_actual, gold_expected)
    report.print_report()
    assert report.passed
