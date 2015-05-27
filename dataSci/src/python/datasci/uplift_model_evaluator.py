import random
import pandas as pd

UPLIFT_DATA_FILE_PATH = '/Users/jon.sondag/code/adServer/model-builder/test/python/files/uplift_model_evaluator/part-00000'
NUM_GROUPS = 20

def gen_data_file(path):
    """
    File format is:
    Model Prediction | Treatment/Control | Did Convert?
    0.035 | 0 | 1
    0.025 | 1 | 0
    0.022 | 1 | 0
    ...etc
    """
    NUMBER_OF_POINTS = 10000
    CHANCE_NOT_PURE = 0.9
    BASE_CONVERSION_RATE = 0.40
    NOT_PURE_COEFFICIENT = 0.01
    MODEL_STANDARD_DEVIATION = 0.005
    HIDDEN_VARIABLE_STANDARD_DEVIATION = 0.1

    with open(path, 'w') as f:
        for _ in range(NUMBER_OF_POINTS):
            hidden_variable = random.normalvariate(0.1, HIDDEN_VARIABLE_STANDARD_DEVIATION)
            is_not_pure = 1 if random.random() < CHANCE_NOT_PURE else 0
            model_prediction = -hidden_variable + random.normalvariate(0, MODEL_STANDARD_DEVIATION)
            did_convert = 1 if random.random() < BASE_CONVERSION_RATE - is_not_pure * (NOT_PURE_COEFFICIENT + hidden_variable) else 0
            write_string = ('\t').join([str(model_prediction), str(is_not_pure), str(did_convert)]) + '\n'
            f.write(write_string)
    f.close()

def evaluate_uplift_model(path):
    df = pd.read_csv(path, sep='\t', header=None)
    df.columns = ['model-prediction', 'is-not-pure', 'did-convert']

    df_pure = df[df['is-not-pure'] == 0]
    df_not_pure = df[df['is-not-pure'] == 1]

    del df_pure['is-not-pure']
    del df_not_pure['is-not-pure']

    overall_diff = (df_pure['did-convert'].mean() - df_not_pure['did-convert'].mean()) * len(df_pure)

    df_pure.sort('model-prediction', ascending=True, inplace=True)
    df_not_pure.sort('model-prediction', ascending=True, inplace=True)


    del df_pure['model-prediction']
    del df_not_pure['model-prediction']

    df_pure['label'] = range(len(df_pure))
    df_not_pure['label'] = range(len(df_not_pure))

    df_pure['label'] = df_pure['label'] / (len(df_pure) / NUM_GROUPS)
    df_not_pure['label'] = df_not_pure['label'] / (len(df_not_pure) / NUM_GROUPS)

    df_pure['label'][df_pure['label'] == NUM_GROUPS] = NUM_GROUPS - 1
    df_not_pure['label'][df_not_pure['label'] == NUM_GROUPS] = NUM_GROUPS - 1

    pure_means_counts = df_pure.groupby('label').agg([np.mean, 'count'])
    not_pure_means_counts = df_not_pure.groupby('label').agg([np.mean, 'count'])

    diffs_by_group = (pure_means_counts['did-convert', 'mean'] - not_pure_means_counts['did-convert', 'mean']) * pure_means_counts['did-convert', 'count']
    diffs_by_group_cumsum = diffs_by_group.cumsum()
    result = diffs_by_group_cumsum / overall_diff
    result.plot()
    text(0.1, 0.95, 'AUUC: ' + str(round(result.mean(), 2)), fontsize=14)


if __name__ == '__main__':
    gen_data_file(UPLIFT_DATA_FILE_PATH)
    # evaluate_uplift_model(UPLIFT_DATA_FILE_PATH)
