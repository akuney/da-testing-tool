import datasci.vwpy


def run_example():
    train_dir = '/Users/jon.sondag/Datasets/GenerateQualityScoreSignalsTask-20141002040501/CT/FLIGHTS/NIL/training/normalized_vw.gz/'
    classifier_model_evaluator = datasci.vwpy.vw_utils.ClassifierModelEvaluator(train_dir, 'qs_model', passes=2)
    test_dir = '/Users/jon.sondag/Datasets/GenerateQualityScoreSignalsTask-20141002040501/CT/FLIGHTS/NIL/testing/normalized_vw.gz/'
    data_file_name = ['part-all.gz']
    train_files = [train_dir + data_file for data_file in data_file_name]
    test_files = [test_dir + data_file for data_file in data_file_name]
    regularization_factors = [1e-6]
    classifier_model_evaluator.evaluate_model(train_files, test_files, regularization_factors)


if __name__ == '__main__':
    # import reimport
    # reimport.reimport(datasci)  # so we don't have to restart ipython in between runs if we're modifying vwpy
    run_example()
