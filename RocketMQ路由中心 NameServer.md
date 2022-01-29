**RocketMQ路由中心 NameServer**

一、NameServer功能

​		NameServer负责RocketMQ中路由功能，消息服务器broker启动时会向NameServer注册并以长连接的方式连接（每隔十秒会扫描一遍broker，剔除宕机的broker，所以broker宕机后NameServer不会立马知道并且剔除），当消息生产者发消息时，会经过NamerServer，然后由NameServer经过负载算法从注册的broker列表中选取一个进行消息发送。

二、NameServer两大配置类（NamesevConfig、NettyServerConfig）

NamesevConfig

![NamesevConfig](E:\Hades-P-Huang\images\NameServerConfig.png)

NettyServerConfig

![NettyServerConfig](E:\Hades-P-Huang\images\NettyServerConfig.png)

三、NameServer启动流程

​	创建NamesrvController

![](E:\Hades-P-Huang\images\NettyServerConfig.png)

初始化启动命令

```java
public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
            new Option("n", "namesrvAddr", true,
                "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
```

封装启动命令

```java
public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
```

解析启动命令

```java
public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
        CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            hf.printHelp(appName, options, true);
            System.exit(1);
        }

        return commandLine;
    }
```

初始化NamesrvConfig、NettyServerConfig配置并设置监听端口为9876（启动时可以通过 ./mqnamesrv -c configFile -p 指定加载的配置文件并打印当前加载的配置属性）

```java
  //初始化配置
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        //初始化网络配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);//默认监听端口9876
        //判断是否有额外的启动指令
        if (commandLine.hasOption('c')) {
            //判断是否有指定的配置文件 有就加载指定的配置文件
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        //判断是否需要打印当前加载的配置文件
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }
```

初始化NamesrvController（加载各工作线程池-定期扫描broker）

```java
public boolean initialize() {

        this.kvConfigManager.load();

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        this.remotingExecutor =
            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

        //定时移除状态不为激活状态的broker  初始化延迟5秒  两次扫描移除间隔10s
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);

        //定时打印kv配置  间隔10分钟
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        //判断服务器ssl是否为不可用
        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
                fileWatchService = new FileWatchService(
                    new String[] {
                        TlsSystemConfig.tlsServerCertPath,
                        TlsSystemConfig.tlsServerKeyPath,
                        TlsSystemConfig.tlsServerTrustCertPath
                    },
                    new FileWatchService.Listener() {
                        boolean certChanged, keyChanged = false;
                        @Override
                        public void onChanged(String path) {
                            if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                log.info("The trust certificate changed, reload the ssl context");
                                reloadServerSslContext();
                            }
                            if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                certChanged = true;
                            }
                            if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                keyChanged = true;
                            }
                            if (certChanged && keyChanged) {
                                log.info("The certificate and private key changed, reload the ssl context");
                                certChanged = keyChanged = false;
                                reloadServerSslContext();
                            }
                        }
                        private void reloadServerSslContext() {
                            ((NettyRemotingServer) remotingServer).loadSslContext();
                        }
                    });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }
```

启动netty连接

```java
 public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                }
            });

        prepareSharableHandlers();

        ServerBootstrap childHandler =
            this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getServerSocketBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark()))
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, handshakeHandler)
                            .addLast(defaultEventExecutorGroup,
                                encoder,
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                connectionManageHandler,
                                serverHandler
                            );
                    }
                });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }
```

