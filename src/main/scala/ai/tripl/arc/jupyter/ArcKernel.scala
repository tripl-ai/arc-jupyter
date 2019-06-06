package ai.tripl.arc.jupyter

import almond.util.ThreadUtil.singleThreadedExecutionContext
import almond.channels.zeromq.ZeromqThreads
import almond.kernel.install.Install
import almond.kernel.{Kernel, KernelThreads}
import almond.logger.{Level, LoggerContext}
import caseapp._

object ArcKernel extends CaseApp[Options] {

  def run(options: Options, args: RemainingArgs): Unit = {

    val logCtx = Level.fromString(options.log) match {
      case Left(err) =>
        Console.err.println(err)
        sys.exit(1)
      case Right(level) =>
        LoggerContext.stderr(level)
    }

    val log = logCtx(getClass)

    if (options.install)
      Install.installOrError(
        defaultId = "arc",
        defaultDisplayName = "Arc",
        language = "arc",
        options = options.installOptions
      ) match {
        case Left(e) =>
          log.debug("Cannot install kernel", e)
          Console.err.println(s"Error: ${e.getMessage}")
          sys.exit(1)
        case Right(dir) =>
          println(s"Installed arc kernel under $dir")
          sys.exit(0)
      }

    val connectionFile = options.connectionFile.getOrElse {
      Console.err.println(
        "No connection file passed, and installation not asked. Run with --install to install the kernel, " +
          "or pass a connection file via --connection-file to run the kernel."
      )
      sys.exit(1)
    }

    val zeromqThreads = ZeromqThreads.create("arc-kernel")
    val kernelThreads = KernelThreads.create("arc-kernel")
    val interpreterEc = singleThreadedExecutionContext("arc-interpreter")

    log.debug("Running kernel")
    Kernel.create(new ArcInterpreter, interpreterEc, kernelThreads, logCtx)
      .flatMap(_.runOnConnectionFile(connectionFile, "arc", zeromqThreads))
      .unsafeRunSync()
  }
}
