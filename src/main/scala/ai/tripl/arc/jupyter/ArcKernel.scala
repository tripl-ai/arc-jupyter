package ai.tripl.arc.jupyter

import almond.util.ThreadUtil.singleThreadedExecutionContext
import almond.channels.zeromq.ZeromqThreads
import almond.kernel.install.Install
import almond.kernel.{Kernel, KernelThreads}
import almond.logger.{Level, LoggerContext}

object ArcKernel {

  def main(args: Array[String]): Unit = {

    // read command line arguments into a map
    // must be in --key=value format
    val clArgs = collection.mutable.Map[String, String]()
    val (opts, _) = args.partition {
      _.startsWith("--")
    }
    opts.map { x =>
      // regex split on only single = signs not at start or end of line
      val pair = x.split("=(?!=)(?!$)", 2)
      if (pair.length == 2) {
        clArgs += (pair(0).split("-{1,2}")(1) -> pair(1))
      }
    }
    val commandLineArguments = clArgs.toMap

    val zeromqThreads = ZeromqThreads.create("arc-kernel")
    val kernelThreads = KernelThreads.create("arc-kernel")
    val interpreterEc = singleThreadedExecutionContext("arc-interpreter")

    Kernel.create(new ArcInterpreter, interpreterEc, kernelThreads, LoggerContext.stderr(Level.Warning))
      .flatMap(_.runOnConnectionFile(commandLineArguments.get("connection").get, "arc", zeromqThreads))
      .unsafeRunSync()
  }
}
