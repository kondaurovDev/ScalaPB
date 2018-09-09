package scalapb.compiler

import com.google.protobuf.Descriptors.{Descriptor, MethodDescriptor, ServiceDescriptor}
import scalapb.compiler.FunctionalPrinter.PrinterEndo

import scala.collection.JavaConverters._

final class GrpcServicePrinter(service: ServiceDescriptor, implicits: DescriptorImplicits) {
  import implicits._
  private[this] def observer(typeParam: String): String = s"$streamObserver[$typeParam]"

  def getSealedName(descriptor: Descriptor): Option[String] = {
    descriptor.getOneofs.asScala.headOption.map(_.getContainingType.getFullName)
  }

  def getScalaIn(method: MethodDescriptor): String = {
    getSealedName(method.getInputType).getOrElse(method.scalaIn)
  }

  def getScalaOut(method: MethodDescriptor): String = {
    getSealedName(method.getOutputType).getOrElse(method.scalaOut)
  }

  private[this] def serviceMethodSignature(method: MethodDescriptor) = {

    val in = getScalaIn(method)
    val out = getScalaOut(method)

    s"def ${method.name}" + (method.streamType match {
      case StreamType.Unary =>
        s"(request: $in): scala.concurrent.Future[$out]"
      case StreamType.ClientStreaming =>
        s"(responseObserver: ${observer(out)}): ${observer(in)}"
      case StreamType.ServerStreaming =>
        s"(request: $in, responseObserver: ${observer(out)}): Unit"
      case StreamType.Bidirectional =>
        s"(responseObserver: ${observer(out)}): ${observer(in)}"
    })
  }

  private[this] def blockingMethodSignature(method: MethodDescriptor) = {

    val in = getScalaIn(method)
    val out = getScalaOut(method)

    s"def ${method.name}" + (method.streamType match {
      case StreamType.Unary =>
        s"(request: $in): $out"
      case StreamType.ServerStreaming =>
        s"(request: $in): scala.collection.Iterator[$out]"
      case _ => throw new IllegalArgumentException("Invalid method type.")
    })

  }

  private[this] def serviceTrait: PrinterEndo = {
    val endos: PrinterEndo = { p =>
      p.seq(service.methods.map(m => serviceMethodSignature(m)))
    }

    p =>
      p.add(s"trait ${service.name} extends _root_.scalapb.grpc.AbstractService {")
        .indent
        .add(s"override def serviceCompanion = ${service.name}")
        .call(endos)
        .outdent
        .add("}")
  }

  private[this] def serviceTraitCompanion: PrinterEndo = { p =>
    p.add(
        s"object ${service.name} extends _root_.scalapb.grpc.ServiceCompanion[${service.name}] {"
      )
      .indent
      .add(
        s"implicit def serviceCompanion: _root_.scalapb.grpc.ServiceCompanion[${service.name}] = this"
      )
      .add(
        s"def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = ${service.getFile.fileDescriptorObjectFullName}.javaDescriptor.getServices().get(${service.getIndex})"
      )
      .outdent
      .add("}")
  }

  private[this] def blockingClientTrait: PrinterEndo = {
    val endos: PrinterEndo = { p =>
      p.seq(service.methods.filter(_.canBeBlocking).map(m => blockingMethodSignature(m)))
    }

    p =>
      p.add(s"trait ${service.blockingClient} {")
        .indent
        .add(s"def serviceCompanion = ${service.name}")
        .call(endos)
        .outdent
        .add("}")
  }

  private[this] val channel     = "_root_.io.grpc.Channel"
  private[this] val callOptions = "_root_.io.grpc.CallOptions"

  private[this] val abstractStub   = "_root_.io.grpc.stub.AbstractStub"
  private[this] val streamObserver = "_root_.io.grpc.stub.StreamObserver"

  private[this] val serverCalls = "_root_.io.grpc.stub.ServerCalls"
  private[this] val clientCalls = "_root_.io.grpc.stub.ClientCalls"

  private[this] val guavaFuture2ScalaFuture = "scalapb.grpc.Grpc.guavaFuture2ScalaFuture"
  private[this] val observerMap = "scalapb.grpc.Grpc.observerMap"

  private[this] def clientMethodImpl(m: MethodDescriptor, blocking: Boolean) =
    PrinterEndo { p =>
      m.streamType match {
        case StreamType.Unary =>
          if (blocking) {
            val signature = "override " + blockingMethodSignature(m) + " = {"
            val request = if (m.getInputType.isSealedOneofType) "request.asMessage" else "request"
            val afterBrackets = if (m.getOutputType.isSealedOneofType) s".to${m.getOutputType.sealedOneofName}" else ""
            p.add(
              signature,
              s"""  $clientCalls.blockingUnaryCall(channel.newCall(${m.descriptorName}, options), $request)$afterBrackets""",
              "}"
            )
          } else {
            val signature =  "override " + serviceMethodSignature(m) + " = {"
            val afterRequest = if (m.getInputType.isSealedOneofType) ".asMessage" else ""
            def mapF = s"f.map(_.to${m.getOutputType.sealedOneofName})(concurrent.ExecutionContext.fromExecutor(options.getExecutor))"
            p.add(
              signature,
              s"""  val f = $guavaFuture2ScalaFuture($clientCalls.futureUnaryCall(channel.newCall(${m.descriptorName}, options), request$afterRequest))""",
              s"  ${ if (!m.getOutputType.isSealedOneofType) "f" else mapF}",
              "}"
            )
          }

        case StreamType.ServerStreaming =>

          if (blocking) {
            val afterBrackets = if (m.getOutputType.isSealedOneofType) s".map(_.to${m.getOutputType.sealedOneofName})" else ""
            p.add(
                "override " + blockingMethodSignature(m) + " = {"
              )
              .addIndented(
                s"scala.collection.JavaConverters.asScalaIteratorConverter($clientCalls.blockingServerStreamingCall(channel.newCall(${m.descriptorName}, options), request)).asScala$afterBrackets"
              )
              .add("}")
          } else {
            val signature = "override " + serviceMethodSignature(m) + " = {"
            if (m.getOutputType.isSealedOneofType) {
              p.add(signature)
                .addIndented(
                  s"val obs = $observerMap[${getScalaOut(m)}, ${m.scalaOut}](responseObserver, (m) => m.to${m.getOutputType.sealedOneofName} )",
                  s"$clientCalls.asyncServerStreamingCall(channel.newCall(${m.descriptorName}, options), request, obs)"
                )
                .add("}")
            } else {
              p.add(signature)
                .addIndented(
                  s"$clientCalls.asyncServerStreamingCall(channel.newCall(${m.descriptorName}, options), request, responseObserver)"
                )
                .add("}")
            }

          }

        case StreamType.ClientStreaming =>

          require(!blocking, s"Method '${m.getFullName}' can't be blocking because it is used in streaming")
          val call = s"$clientCalls.asyncClientStreamingCall"

          if (m.getInputType.isSealedOneofType) {
            p.add("override " + serviceMethodSignature(m) + " = {")
              .addIndented(
                s"{11}; val obs = $observerMap[${m.scalaIn}, ${getScalaIn(m)}](responseObserver, (m) => m.asMessage )",
                s"$call(channel.newCall(${m.descriptorName}, options), obs)"
              )
              .add("}")
          } else if (m.getOutputType.isSealedOneofType)  {
            p.add("override " + serviceMethodSignature(m) + " = {")
              .addIndented(
                s"{22}; val obs = $observerMap[${m.scalaIn}, ${getScalaIn(m)}](responseObserver, (m) => m.asMessage )",
                s"$call(channel.newCall(${m.descriptorName}, options), obs)"
              )
              .add("}")
          } else {
            p.add("override " + serviceMethodSignature(m) + " = {")
              .addIndented(
                s"{33}; $call(channel.newCall(${m.descriptorName}, options), responseObserver)"
              )
              .add("}")
          }

        case StreamType.Bidirectional =>

          require(!blocking, s"Method '${m.getFullName}' can't be blocking because it is used in streaming")
          val call = s"$clientCalls.asyncBidiStreamingCall"

          if (m.getInputType.isSealedOneofType) {
            p.add("override " + serviceMethodSignature(m) + " = {")
              .addIndented(
                "{1};",
                s"val obs = $call(channel.newCall(${m.descriptorName}, options), responseObserver)",
                s"$observerMap[${m.scalaIn}, ${getScalaIn(m)}](obs, (m) => m.asMessage )",
                s""
              )
              .add("}")
          } else if (m.getOutputType.isSealedOneofType)  {
            p.add("override " + serviceMethodSignature(m) + " = {")
              .addIndented(
                s"{2}; val obs = $observerMap[${m.scalaIn}, ${getScalaIn(m)}](responseObserver, (m) => m.asMessage )",
                s"$call(channel.newCall(${m.descriptorName}, options), obs)"
              )
              .add("}")
          } else {
            p.add("override " + serviceMethodSignature(m) + " = {")
              .addIndented(
                s"{3}; $call(channel.newCall(${m.descriptorName}, options), responseObserver)"
              )
              .add("}")
          }

/*
      val obs = _root_.io.grpc.stub.ClientCalls.asyncBidiStreamingCall(channel.newCall(METHOD_PROCESS_DATA_STREAM, options), responseObserver)
      scalapb.grpc.Grpc.observerMap[data_clean.data_processor.response.ResponseMessage, data_clean.data_processor.response.Response](obs, (m) => m.asMessage )

 */


      }
    } andThen PrinterEndo { _.newline }

  private def stubImplementation(
      className: String,
      baseClass: String,
      methods: Seq[PrinterEndo]
  ): PrinterEndo = { p =>
    val build =
      s"override def build(channel: $channel, options: $callOptions): ${className} = new $className(channel, options)"
    p.add(
        s"class $className(channel: $channel, options: $callOptions = $callOptions.DEFAULT) extends $abstractStub[$className](channel, options) with $baseClass {"
      )
      .indent
      .call(methods: _*)
      .add(build)
      .outdent
      .add("}")
  }

  private[this] val blockingStub: PrinterEndo = {
    val methods = service.methods.filter(_.canBeBlocking).map(clientMethodImpl(_, true))
    stubImplementation(service.blockingStub, service.blockingClient, methods)
  }

  private[this] val stub: PrinterEndo = {
    val methods = service.getMethods.asScala.map(clientMethodImpl(_, false))
    stubImplementation(service.stub, service.name, methods)
  }

  private[this] def methodDescriptor(method: MethodDescriptor) = PrinterEndo { p =>
    def marshaller(typeName: String) =
      s"new scalapb.grpc.Marshaller($typeName)"

    val methodType = method.streamType match {
      case StreamType.Unary           => "UNARY"
      case StreamType.ClientStreaming => "CLIENT_STREAMING"
      case StreamType.ServerStreaming => "SERVER_STREAMING"
      case StreamType.Bidirectional   => "BIDI_STREAMING"
    }

    val grpcMethodDescriptor = "_root_.io.grpc.MethodDescriptor"

    p.addStringMargin(
      s"""val ${method.descriptorName}: $grpcMethodDescriptor[${method.scalaIn}, ${method.scalaOut}] =
      |  $grpcMethodDescriptor.newBuilder()
      |    .setType($grpcMethodDescriptor.MethodType.$methodType)
      |    .setFullMethodName($grpcMethodDescriptor.generateFullMethodName("${service.getFullName}", "${method.getName}"))
      |    .setSampledToLocalTracing(true)
      |    .setRequestMarshaller(${marshaller(method.scalaIn)})
      |    .setResponseMarshaller(${marshaller(method.scalaOut)})
      |    .build()
      |"""
    )
  }

  private[this] def serviceDescriptor(service: ServiceDescriptor) = {

    val grpcServiceDescriptor = "_root_.io.grpc.ServiceDescriptor"

    PrinterEndo(
      _.add(s"val ${service.descriptorName}: $grpcServiceDescriptor =").indent
        .add(s"""$grpcServiceDescriptor.newBuilder("${service.getFullName}")""")
        .indent
        .add(
          s""".setSchemaDescriptor(new _root_.scalapb.grpc.ConcreteProtoFileDescriptorSupplier(${service.getFile.fileDescriptorObjectFullName}.javaDescriptor))"""
        )
        .print(service.methods) {
          case (p, method) =>
            p.add(s".addMethod(${method.descriptorName})")
        }
        .add(".build()")
        .outdent
        .outdent
        .newline
    )
  }

  private[this] def addMethodImplementation(method: MethodDescriptor): PrinterEndo = PrinterEndo {
    _.add(".addMethod(")
      .add(s"  ${method.descriptorName},")
      .indent
      .call(PrinterEndo { p =>
        val call = method.streamType match {
          case StreamType.Unary           => s"$serverCalls.asyncUnaryCall"
          case StreamType.ClientStreaming => s"$serverCalls.asyncClientStreamingCall"
          case StreamType.ServerStreaming => s"$serverCalls.asyncServerStreamingCall"
          case StreamType.Bidirectional   => s"$serverCalls.asyncBidiStreamingCall"
        }

        val executionContext = "executionContext"
        val serviceImpl      = "serviceImpl"

        method.streamType match {
          case StreamType.Unary =>
            val serverMethod = s"$serverCalls.UnaryMethod[${method.scalaIn}, ${method.scalaOut}]"
            val afterRequest = if (method.getInputType.isSealedOneofType) s".to${method.getInputType.sealedOneofName}" else ""
            val afterT = if (method.getOutputType.isSealedOneofType) ".map(_.asMessage)" else ""

            p.addStringMargin(s"""$call(new $serverMethod {
             |  override def invoke(request: ${method.scalaIn}, observer: $streamObserver[${method.scalaOut}]): Unit =
             |    $serviceImpl.${method.name}(request$afterRequest).onComplete(t => scalapb.grpc.Grpc.completeObserver(observer)(t$afterT))(
             |      $executionContext)
             |}))""")

          case StreamType.ServerStreaming =>
            val serverMethod =
              s"$serverCalls.ServerStreamingMethod[${method.scalaIn}, ${method.scalaOut}]"

            if (method.getOutputType.isSealedOneofType) {

              p.addStringMargin(s"""$call(new $serverMethod {
               |  override def invoke(request: ${method.scalaIn}, observer: $streamObserver[${method.scalaOut}]): Unit = {
               |   val obs = $observerMap[${method.scalaOut}, ${getScalaOut(method)}](observer, (m) => m.asMessage )
               |   $serviceImpl.${method.name}(request, obs)
               |  }
               |}))""")

            } else if (method.getInputType.isSealedOneofType) {

              p.addStringMargin(s"""$call(new $serverMethod {
               |  override def invoke(request: ${method.scalaIn}, observer: $streamObserver[${method.scalaOut}]): Unit = {
               |   {} val obs = $observerMap[${method.scalaOut}, ${getScalaOut(method)}](observer, (m) => m.asMessage )
               |   $serviceImpl.${method.name}(request, obs)
               |  }
               |}))""")

            } else {

              p.addStringMargin(s"""$call(new $serverMethod {
               |  override def invoke(request: ${method.scalaIn}, observer: $streamObserver[${method.scalaOut}]): Unit = {
               |   $serviceImpl.${method.name}(request, observer)
               |  }
               |}))""")


            }

          case StreamType.ClientStreaming =>

            val serverMethod = s"$serverCalls.ClientStreamingMethod[${method.scalaIn}, ${method.scalaOut}]"

            p.addStringMargin(s"""$call(new $serverMethod {
             |  override def invoke(observer: $streamObserver[${method.scalaOut}]): $streamObserver[${method.scalaIn}] = {
             |   val obs = $observerMap[${method.scalaOut}, ${getScalaOut(method)}](observer, (m) => m.asMessage )
             |   $serviceImpl.${method.name}(obs)
             |  }
             |
            |}))""")

          case StreamType.Bidirectional =>

            val serverMethod = s"$serverCalls.BidiStreamingMethod[${method.scalaIn}, ${method.scalaOut}]"

            /*
                    override def invoke(observer: _root_.io.grpc.stub.StreamObserver[data_clean.data_processor.response2.Response2Message]): _root_.io.grpc.stub.StreamObserver[data_clean.data_processor.response.ResponseMessage] = {
          val obs = scalapb.grpc.Grpc.observerMap[data_clean.data_processor.response2.Response2Message, data_clean.data_processor.response2.Response2](obs, (m) => m.asMessage )
          val obs2 = serviceImpl.processDataStream(obs)
          scalapb.grpc.Grpc.observerMap[data_clean.data_processor.response.Response, data_clean.data_processor.response.ResponseMessage](obs2, (v) => v.toResponse)

             */

            if (method.getOutputType.isSealedOneofType) {
              p.addStringMargin(s"""$call(new $serverMethod {
               |  {111};
               |  override def invoke(observer: $streamObserver[${method.scalaOut}]): $streamObserver[${method.scalaIn}] = {
               |   val obs = $observerMap[${method.scalaOut}, ${getScalaOut(method)}](observer, (m) => m.asMessage )
               |   val obs2 = $serviceImpl.${method.name}(obs)
               |   $observerMap[${getScalaIn(method)}, ${method.scalaIn}](obs2, (m) => m.to${method.getInputType.sealedOneofName} )
               |  }
               |
              |}))""")

            } else if (method.getInputType.isSealedOneofType) {
              p.addStringMargin(s"""$call(new $serverMethod {
               |  {222};
               |  override def invoke(observer: $streamObserver[${method.scalaOut}]): $streamObserver[${method.scalaIn}] = {
               |   val obs = $serviceImpl.${method.name}(observer)
               |   $observerMap[${getScalaIn(method)}, ${method.scalaIn}](obs, (m) => m.to${method.getInputType.sealedOneofName} )
               |  }
               |
            |}))""")
            } else {
              p.addStringMargin(s"""$call(new $serverMethod {
               |  {333};
               |
               |  override def invoke(observer: $streamObserver[${method.scalaOut}]): $streamObserver[${method.scalaIn}] = {
               |   $serviceImpl.${method.name}(observer)
               |  }
               |
               |}))""")
            }

        }

      })
      .outdent
  }

  private[this] val bindService = {
    val executionContext = "executionContext"
    val methods          = service.methods.map(addMethodImplementation)
    val serverServiceDef = "_root_.io.grpc.ServerServiceDefinition"

    PrinterEndo(
      _.add(
        s"""def bindService(serviceImpl: ${service.name}, $executionContext: scala.concurrent.ExecutionContext): $serverServiceDef ="""
      ).indent
        .add(s"""$serverServiceDef.builder(${service.descriptorName})""")
        .call(methods: _*)
        .add(".build()")
        .outdent
    )
  }

  def printService(printer: FunctionalPrinter): FunctionalPrinter = {
    printer
      .add("package " + service.getFile.scalaPackageName, "", s"object ${service.objectName} {")
      .indent
      .call(service.methods.map(methodDescriptor): _*)
      .call(serviceDescriptor(service))
      .call(serviceTrait)
      .newline
      .call(serviceTraitCompanion)
      .newline
      .call(blockingClientTrait)
      .newline
      .call(blockingStub)
      .newline
      .call(stub)
      .newline
      .call(bindService)
      .newline
      .add(
        s"def blockingStub(channel: $channel): ${service.blockingStub} = new ${service.blockingStub}(channel)"
      )
      .newline
      .add(s"def stub(channel: $channel): ${service.stub} = new ${service.stub}(channel)")
      .newline
      .add(
        s"def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = ${service.getFile.fileDescriptorObjectFullName}.javaDescriptor.getServices().get(${service.getIndex})"
      )
      .newline
      .outdent
      .add("}")
  }
}
