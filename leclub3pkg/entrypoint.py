from db4ml import execute
from leclub3pkg.common import Job


class SampleJob(Job):
    def launch(self):
        self.logger.info("Launching sample job")

        execute("leclub3model_train", self.spark, self.config_path)
        execute("leclub3model_scoring_pandas", self.spark, self.config_path)

        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    job = SampleJob()
    job.launch()
