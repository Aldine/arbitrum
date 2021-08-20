/*
 * Copyright 2021, Offchain Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package configuration

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/providers/s3"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
)

var logger = log.With().Caller().Stack().Str("component", "configuration").Logger()

var (
	arbitrumOneRollupAddress    = "0xC12BA48c781F6e392B49Db2E25Cd0c28cD77531A"
	rinkebyTestnetRollupAddress = "0xFe2c86CF40F89Fe2F726cFBBACEBae631300b50c"
)

type Conf struct {
	Dump      bool   `koanf:"dump"`
	EnvPrefix string `koanf:"env-prefix"`
	File      string `koanf:"file"`
	S3        S3     `koanf:"s3"`
	String    string `koanf:"string"`
}

func addConfOptions(f *flag.FlagSet, prefix string) {
	f.Bool(prefix+"dump", false, "print out currently active configuration file")
	f.String(prefix+"env-prefix", "", "environment variables with given prefix will be loaded as configuration values")
	f.String(prefix+"file", "", "name of configuration file")
	addS3Options(f, prefix+"s3.")
	f.String(prefix+"string", "", "configuration as JSON string")
}

type S3 struct {
	AccessKey string `koanf:"access-key"`
	Bucket    string `koanf:"bucket"`
	ObjectKey string `koanf:"object-key"`
	Region    string `koanf:"region"`
	SecretKey string `koanf:"secret-key"`
}

func addS3Options(f *flag.FlagSet, prefix string) {
	f.String(prefix+"access-key", "", "S3 access key")
	f.String(prefix+"secret-key", "", "S3 secret key")
	f.String(prefix+"region", "", "S3 region")
	f.String(prefix+"bucket", "", "S3 bucket")
	f.String(prefix+"object-key", "", "S3 object key")
}

func loadS3Variables(k *koanf.Koanf, prefix string) error {
	return k.Load(s3.Provider(s3.Config{
		AccessKey: k.String(prefix + "access-key"),
		SecretKey: k.String(prefix + "secret-key"),
		Region:    k.String(prefix + "region"),
		Bucket:    k.String(prefix + "bucket"),
		ObjectKey: k.String(prefix + "object-key"),
	}), nil)
}

type Core struct {
	Cache                  CoreCache     `koanf:"cache"`
	CheckpointLoadGasCost  int           `koanf:"checkpoint-load-gas-cost"`
	Debug                  bool          `koanf:"debug"`
	GasCheckpointFrequency int           `koanf:"gas-checkpoint-frequency"`
	MessageProcessCount    int           `koanf:"message-process-count"`
	SaveRocksdbInterval    time.Duration `koanf:"save-rocksdb-interval"`
	SaveRocksdbPath        string        `koanf:"save-rocksdb-path"`
}

func addCoreOptions(f *flag.FlagSet, prefix string) {
	f.Duration(prefix+"save-rocksdb-interval", 0, "duration between saving database backups, 0 to disable")
	f.String(prefix+"save-rocksdb-path", "db_checkpoints", "path to save database backups in")
}

type CoreCache struct {
	LRUSize     int           `koanf:"lru-size"`
	TimedExpire time.Duration `koanf:"timed-expire"`
}

// DefaultCoreSettings is useful in unit tests
func DefaultCoreSettings() *Core {
	return &Core{
		Cache: CoreCache{
			LRUSize:     1000,
			TimedExpire: 20 * time.Minute,
		},
		CheckpointLoadGasCost:  1_000_000,
		GasCheckpointFrequency: 1_000_000,
		MessageProcessCount:    10,
	}
}

type Endpoint struct {
	Addr string `koanf:"addr"`
	Port string `koanf:"port"`
	Path string `koanf:"path"`
}

func addEndpointOptions(f *flag.FlagSet, commandPrefix, helpPrefix string, defaultAddr, defaultPort string) {
	f.String(commandPrefix+"addr", defaultAddr, helpPrefix+" address")
	f.String(commandPrefix+"port", defaultPort, helpPrefix+" port")
	f.String(commandPrefix+"path", "/", helpPrefix+" path")
}

type FeedInput struct {
	Timeout time.Duration `koanf:"timeout"`
	URLs    []string      `koanf:"url"`
}

func addFeedInputOptions(f *flag.FlagSet, prefix string) {
	f.Duration(prefix+"timeout", 20*time.Second, "duration to wait before timing out connection to server")
	f.StringSlice(prefix+"url", []string{}, "URL of sequencer feed source")
}

type FeedOutput struct {
	Addr          string        `koanf:"addr"`
	IOTimeout     time.Duration `koanf:"io-timeout"`
	Port          string        `koanf:"port"`
	Ping          time.Duration `koanf:"ping"`
	ClientTimeout time.Duration `koanf:"client-timeout"`
	Queue         int           `koanf:"queue"`
	Workers       int           `koanf:"workers"`
}

func addFeedOutputOptions(f *flag.FlagSet, k *koanf.Koanf, prefix string) error {
	f.String(prefix+"addr", "0.0.0.0", "address to bind the relay feed output to")
	f.Duration(prefix+"io-timeout", 5*time.Second, "duration to wait before timing out HTTP to WS upgrade")
	f.Int(prefix+"port", 9642, "port to bind the relay feed output to")
	f.Duration(prefix+"ping", 5*time.Second, "duration for ping interval")
	f.Duration(prefix+"client-timeout", 15*time.Second, "duraction to wait before timing out connections to client")
	f.Int(prefix+"workers", 100, "Number of threads to reserve for HTTP to WS upgrade")

	return k.Load(confmap.Provider(map[string]interface{}{
		prefix + "queue": 100,
	}, "."), nil)
}

type Feed struct {
	Input  FeedInput  `koanf:"input"`
	Output FeedOutput `koanf:"output"`
}

type Healthcheck struct {
	Enable   bool     `koanf:"enable"`
	Endpoint Endpoint `koanf:"endpoint"`

	MaxInboxSyncDiff         int64  `koanf:"max-inbox-sync-diff"`
	MaxMessagesSyncDiff      int64  `koanf:"max-messages-sync-diff"`
	MaxLogsProcessedSyncDiff int64  `koanf:"max-logs-processed-sync-diff"`
	MaxL1BlockDiff           int64  `koanf:"max-l1-block-diff"`
	MaxL2BlockDiff           int64  `koanf:"max-l2-block-diff"`
	ForwarderReadyURL        string `koanf:"forwarder-ready-url"`
}

func addHealthcheckOptions(f *flag.FlagSet, k *koanf.Koanf, prefix string) error {
	f.Bool(prefix+"enable", false, "enable healthcheck endpoint")
	addEndpointOptions(f, prefix+"endpoint.", "healthcheck", "127.0.0.1", "8080")

	f.String(prefix+"forwarder-ready-url", "", "address to use for forwarder target healthcheck")

	return k.Load(confmap.Provider(map[string]interface{}{
		prefix + "max-inbox-sync-diff":          100,
		prefix + "max-messages-sync-diff":       100,
		prefix + "max-logs-processed-sync-diff": 100,
		prefix + "max-l1-block-diff":            10,
		prefix + "max-l2-block-diff":            100,
	}, "."), nil)
}

type MetricsServer struct {
	Enable   bool     `koanf:"enable"`
	Prefix   string   `koanf:"prefix"`
	Endpoint Endpoint `koanf:"endpoint"`
}

func addMetricsServerOptions(f *flag.FlagSet, prefix string) {
	f.Bool(prefix+"enable", false, "enable metrics server")
	f.String(prefix+"prefix", "", "prefix for arbitrum metrics")
	addEndpointOptions(f, prefix+"endpoint.", "metrics server", "127.0.0.1", "6070")
}

type Lockout struct {
	Redis         string        `koanf:"redis"`
	SelfRPCURL    string        `koanf:"self-rpc-url"`
	Timeout       time.Duration `koanf:"timeout"`
	MaxLatency    time.Duration `koanf:"max-latency"`
	SeqNumTimeout time.Duration `koanf:"seq-num-timeout"`
}

func addLockoutOptions(f *flag.FlagSet, k *koanf.Koanf, prefix string) error {
	f.String(prefix+"redis", "", "sequencer lockout redis instance URL")
	f.String(prefix+"self-rpc-url", "", "own RPC URL for other sequencers to failover to")

	return k.Load(confmap.Provider(map[string]interface{}{
		prefix + "timeout":         30 * time.Second,
		prefix + "max-latency":     10 * time.Second,
		prefix + "seq-num-timeout": 5 * time.Minute,
	}, "."), nil)
}

type Aggregator struct {
	InboxAddress string `koanf:"inbox-address"`
	MaxBatchTime int64  `koanf:"max-batch-time"`
	Stateful     bool   `koanf:"stateful"`
}

func addAggregatorOptions(f *flag.FlagSet, prefix string) {
	f.String(prefix+"inbox-address", "", "address of the inbox contract")
	f.Int(prefix+"max-batch-time", 10, "max-batch-time=NumSeconds")
	f.Bool(prefix+"stateful", false, "enable pending state tracking")
}

type Sequencer struct {
	CreateBatchBlockInterval          int64   `koanf:"create-batch-block-interval"`
	ContinueBatchPostingBlockInterval int64   `koanf:"continue-batch-posting-block-interval"`
	DelayedMessagesTargetDelay        int64   `koanf:"delayed-messages-target-delay"`
	ReorgOutHugeMessages              bool    `koanf:"reorg-out-huge-messages"`
	Lockout                           Lockout `koanf:"lockout"`
}

func addSequencerOptions(f *flag.FlagSet, k *koanf.Koanf, prefix string) error {
	f.Int64(prefix+"create-batch-block-interval", 270, "block interval at which to create new batches")
	f.Int64(prefix+"continue-batch-posting-block-interval", 2, "block interval to post the next batch after posting a partial one")
	f.Int64(prefix+"delayed-messages-target-delay", 12, "delay before sequencing delayed messages")
	f.Bool(prefix+"reorg-out-huge-messages", false, "erase any huge messages in database that cannot be published (DANGEROUS)")
	return addLockoutOptions(f, k, prefix+"lockout.")
}

type Forwarder struct {
	Target    string `koanf:"target"`
	Submitter string `koanf:"submitter-address"`
	RpcMode   string `koanf:"rpc-mode"`
}

func addForwarderOptions(f *flag.FlagSet, prefix string) {
	f.String(prefix+"target", "", "url of another node to send transactions through")
	f.String(prefix+"submitter-address", "", "address of the node that will submit your transaction to the chain")
	f.String(prefix+"rpc-mode", "full", "RPC mode: either full, non-mutating (no eth_sendRawTransaction), or forwarding-only (only requests forwarded upstream are permitted)")
}

type Node struct {
	Aggregator Aggregator `koanf:"aggregator"`
	ChainID    uint64     `koanf:"chain-id"`
	Forwarder  Forwarder  `koanf:"forwarder"`
	RPC        Endpoint   `koanf:"rpc"`
	Sequencer  Sequencer  `koanf:"sequencer"`
	Type       string     `koanf:"type"`
	WS         Endpoint   `koanf:"ws"`
}

func addNodeOptions(f *flag.FlagSet, k *koanf.Koanf, prefix string) error {
	addAggregatorOptions(f, prefix+"aggregator.")
	addForwarderOptions(f, prefix+"forwarder.")
	addEndpointOptions(f, prefix+"rpc.", "RPC", "0.0.0.0", "8547")
	addEndpointOptions(f, prefix+"ws.", "websocket", "0.0.0.0", "8548")
	if err := addSequencerOptions(f, k, prefix+"sequencer."); err != nil {
		return err
	}
	f.String(prefix+"type", "forwarder", "forwarder, aggregator or sequencer")
	f.Uint64(prefix+"chain-id", 42161, "chain id of the arbitrum chain")
	return nil
}

type Persistent struct {
	Chain        string `koanf:"chain"`
	GlobalConfig string `koanf:"global-config"`
}

func addPersistentOptions(f *flag.FlagSet, prefix string) {
	f.String(prefix+"global-config", ".arbitrum", "location global configuration is located")
	f.String(prefix+"chain", "", "path that chain specific state is located")
}

type Rollup struct {
	Address   string `koanf:"address"`
	FromBlock int64  `koanf:"from-block"`
	Machine   struct {
		Filename string `koanf:"filename"`
		URL      string `koanf:"url"`
	} `koanf:"machine"`
}

func addRollupOptions(f *flag.FlagSet, prefix string) {
	f.String(prefix+"address", "", "layer 2 rollup contract address")
	f.String(prefix+"machine.filename", "", "file to load machine from")
}

type Validator struct {
	Strategy             string `koanf:"strategy"`
	UtilsAddress         string `koanf:"utils-address"`
	WalletFactoryAddress string `koanf:"wallet-factory-address"`
}

func addValidatorOptions(f *flag.FlagSet, prefix string) {
	f.String(prefix+"strategy", "StakeLatest", "strategy for validator to use")
	f.String(prefix+"utils-address", "", "strategy for validator to use")
	f.String(prefix+"wallet-factory-address", "", "strategy for validator to use")
}

type Wallet struct {
	Password string `koanf:"password"`
}

type Log struct {
	RPC  string `koanf:"rpc"`
	Core string `koanf:"core"`
}

func addLogOptions(f *flag.FlagSet, prefix string) {
	f.String(prefix+"rpc", "info", "log level for rpc")
	f.String(prefix+"core", "info", "log level for general arb node logging")
}

type Config struct {
	BridgeUtilsAddress string      `koanf:"bridge-utils-address"`
	Conf               Conf        `koanf:"conf"`
	Core               Core        `koanf:"core"`
	Feed               Feed        `koanf:"feed"`
	GasPrice           float64     `koanf:"gas-price"`
	Healthcheck        Healthcheck `koanf:"healthcheck"`
	L1                 struct {
		URL string `koanf:"url"`
	} `koanf:"l1"`
	Log           Log        `koanf:"log"`
	Node          Node       `koanf:"node"`
	Persistent    Persistent `koanf:"persistent"`
	PProfEnable   bool       `koanf:"pprof-enable"`
	Rollup        Rollup     `koanf:"rollup"`
	Validator     Validator  `koanf:"validator"`
	WaitToCatchUp bool       `koanf:"wait-to-catch-up"`
	Wallet        Wallet     `koanf:"wallet"`

	// The following field needs to be top level for compatibility with the underlying go-ethereum lib
	Metrics       bool          `koanf:"metrics"`
	MetricsServer MetricsServer `koanf:"metrics-server"`
}

func (c *Config) GetNodeDatabasePath() string {
	return path.Join(c.Persistent.Chain, "db")
}

func (c *Config) GetValidatorDatabasePath() string {
	return path.Join(c.Persistent.Chain, "validator_db")
}

func ParseNode(ctx context.Context, args []string) (*Config, *Wallet, string, *big.Int, error) {
	k := koanf.New(".")
	f := flag.NewFlagSet("", flag.ContinueOnError)

	if err := addFeedOutputOptions(f, k, "feed.output."); err != nil {
		return nil, nil, "", nil, err
	}
	if err := addNodeOptions(f, k, "node."); err != nil {
		return nil, nil, "", nil, err
	}
	return parseNonRelay(ctx, f, k, args)
}

func ParseValidator(ctx context.Context, args []string) (*Config, *Wallet, string, *big.Int, error) {
	k := koanf.New(".")
	f := flag.NewFlagSet("", flag.ContinueOnError)

	if err := addFeedOutputOptions(f, k, "feed.output."); err != nil {
		return nil, nil, "", nil, err
	}
	addValidatorOptions(f, "validator.")

	return parseNonRelay(ctx, f, k, args)
}

func parseNonRelay(ctx context.Context, f *flag.FlagSet, k *koanf.Koanf, args []string) (*Config, *Wallet, string, *big.Int, error) {
	addCoreOptions(f, "core.")

	f.String("bridge-utils-address", "", "bridgeutils contract address")

	f.Float64("gas-price", 0, "float of gas price to use in gwei (0 = use L1 node's recommended value)")

	addRollupOptions(f, "rollup.")

	f.String("l1.url", "", "layer 1 ethereum node RPC URL")

	addPersistentOptions(f, "persistent.")

	f.Bool("wait-to-catch-up", false, "wait to catch up to the chain before opening the RPC")

	f.String("wallet.password", "", "password for wallet")

	if err := beginCommonParse(f, k, args); err != nil {
		return nil, nil, "", nil, err
	}

	l1URL := k.String("l1.url")
	if len(l1URL) == 0 {
		return nil, nil, "", nil, errors.New("required parameter --l1.url is missing")
	}

	l1Client, err := ethclient.Dial(l1URL)
	if err != nil {
		return nil, nil, "", nil, errors.Wrapf(err, "error connecting to ethereum L1 node: %s", l1URL)
	}

	var l1ChainId *big.Int
	for {
		l1ChainId, err = l1Client.ChainID(ctx)
		if err == nil {
			break
		}
		logger.Warn().Err(err).Msg("Error getting chain ID")

		select {
		case <-ctx.Done():
			return nil, nil, "", nil, errors.New("ctx cancelled getting chain ID")
		case <-time.After(5 * time.Second):
		}
	}
	logger.Info().Str("l1url", l1URL).Str("chainid", l1ChainId.String()).Msg("connected to l1 chain")

	rollupAddress := k.String("rollup.address")
	if len(rollupAddress) != 0 {
		logger.Info().Str("rollup", rollupAddress).Msg("using custom rollup address")
	} else {
		if l1ChainId.Cmp(big.NewInt(1)) == 0 {
			err := k.Load(confmap.Provider(map[string]interface{}{
				"bridge-utils-address":             "0x84efa170dc6d521495d7942e372b8e4b2fb918ec",
				"feed.input.url":                   []string{"wss://arb1.arbitrum.io/feed"},
				"node.aggregator.inbox-address":    "0x4Dbd4fc535Ac27206064B68FfCf827b0A60BAB3f",
				"node.chain-id":                    "42161",
				"node.forwarder.target":            "https://arb1.arbitrum.io/rpc",
				"persistent.chain":                 "mainnet",
				"rollup.address":                   arbitrumOneRollupAddress,
				"rollup.from-block":                "12525700",
				"rollup.machine.filename":          "mainnet.arb1.mexe",
				"rollup.machine.url":               "https://raw.githubusercontent.com/OffchainLabs/arb-os/48bdb999a703575d26a856499e6eb3e17691e99d/arb_os/arbos.mexe",
				"validator.utils-address":          "0x2B36F23ce0bAbD57553b26Da4C7a0585bac65DC1",
				"validator.wallet-factory-address": "0xe17d8Fa6BC62590f840C5Dd35f300F77D55CC178",
			}, "."), nil)

			if err != nil {
				return nil, nil, "", nil, errors.Wrap(err, "error setting mainnet.arb1 rollup parameters")
			}
		} else if l1ChainId.Cmp(big.NewInt(4)) == 0 {
			err := k.Load(confmap.Provider(map[string]interface{}{
				"bridge-utils-address":             "0xA556F0eF1A0E37a7837ceec5527aFC7771Bf9a67",
				"feed.input.url":                   []string{"wss://rinkeby.arbitrum.io/feed"},
				"node.aggregator.inbox-address":    "0x578BAde599406A8fE3d24Fd7f7211c0911F5B29e",
				"node.chain-id":                    "421611",
				"node.forwarder.target":            "https://rinkeby.arbitrum.io/rpc",
				"persistent.chain":                 "rinkeby",
				"rollup.address":                   rinkebyTestnetRollupAddress,
				"rollup.from-block":                "8700589",
				"rollup.machine.filename":          "testnet.rinkeby.mexe",
				"rollup.machine.url":               "https://raw.githubusercontent.com/OffchainLabs/arb-os/26ab8d7c818681c4ee40792aeb12981a8f2c3dfa/arb_os/arbos.mexe",
				"validator.utils-address":          "0xbb14D9837f6E596167638Ba0963B9Ba8351F68CD",
				"validator.wallet-factory-address": "0x5533D1578a39690B6aC692673F771b3fc668f0a3",
			}, "."), nil)

			if err != nil {
				return nil, nil, "", nil, errors.Wrap(err, "error setting testnet.rinkeby rollup parameters")
			}
		} else {
			return nil, nil, "", nil, fmt.Errorf("connected to unrecognized ethereum network with chain ID: %v", l1ChainId)
		}
	}

	if err := applyOverrides(f, k); err != nil {
		return nil, nil, "", nil, err
	}

	out, wallet, err := endCommonParse(k)
	if err != nil {
		return nil, nil, "", nil, err
	}

	// Fixup directories
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, "", nil, errors.Wrap(err, "Unable to read users home directory")
	}

	// Make persistent storage directory relative to home directory if not already absolute
	if !filepath.IsAbs(out.Persistent.GlobalConfig) {
		out.Persistent.GlobalConfig = path.Join(homeDir, out.Persistent.GlobalConfig)
	}
	err = os.MkdirAll(out.Persistent.GlobalConfig, os.ModePerm)
	if err != nil {
		return nil, nil, "", nil, errors.Wrap(err, "Unable to create global configuration directory")
	}

	// Make chain directory relative to persistent storage directory if not already absolute
	if !filepath.IsAbs(out.Persistent.Chain) {
		out.Persistent.Chain = path.Join(out.Persistent.GlobalConfig, out.Persistent.Chain)
	}
	err = os.MkdirAll(out.Persistent.Chain, os.ModePerm)
	if err != nil {
		return nil, nil, "", nil, errors.Wrap(err, "Unable to create chain directory")
	}

	if len(out.Rollup.Machine.Filename) == 0 {
		// Machine not provided, so use default
		out.Rollup.Machine.Filename = path.Join(out.Persistent.Chain, "arbos.mexe")
	}

	// Make rocksdb backup directory relative to persistent storage directory if not already absolute
	if !filepath.IsAbs(out.Core.SaveRocksdbPath) {
		out.Core.SaveRocksdbPath = path.Join(out.Persistent.Chain, out.Core.SaveRocksdbPath)
	}

	// Make machine relative to storage directory if not already absolute
	out.Rollup.Machine.Filename = path.Join(out.Persistent.GlobalConfig, out.Rollup.Machine.Filename)

	_, err = os.Stat(out.Rollup.Machine.Filename)
	if os.IsNotExist(err) && len(out.Rollup.Machine.URL) != 0 {
		// Machine does not exist, so load it from provided URL
		logger.Debug().Str("URL", out.Rollup.Machine.URL).Msg("downloading machine")

		resp, err := http.Get(out.Rollup.Machine.URL)
		if err != nil {
			return nil, nil, "", nil, errors.Wrapf(err, "unable to get machine from: %s", out.Rollup.Machine.URL)
		}
		if resp.StatusCode != 200 {
			return nil, nil, "", nil, fmt.Errorf("HTTP status '%v' when trying to get machine from: %s", resp.Status, out.Rollup.Machine.URL)
		}

		fileOut, err := os.Create(out.Rollup.Machine.Filename)
		if err != nil {
			return nil, nil, "", nil, errors.Wrapf(err, "unable to open file '%s' for machine", out.Rollup.Machine.Filename)
		}

		_, err = io.Copy(fileOut, resp.Body)
		if err != nil {
			return nil, nil, "", nil, errors.Wrapf(err, "unable to output machine to: %s", out.Rollup.Machine.Filename)
		}
	}

	return out, wallet, l1URL, l1ChainId, nil
}

func ParseRelay(args []string) (*Config, error) {
	k := koanf.New(".")
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	if err := addFeedOutputOptions(f, k, "feed.output."); err != nil {
		return nil, err
	}
	if err := beginCommonParse(f, k, args); err != nil {
		return nil, err
	}

	out, _, err := endCommonParse(k)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func beginCommonParse(f *flag.FlagSet, k *koanf.Koanf, args []string) error {
	addConfOptions(f, "conf.")
	addFeedInputOptions(f, "feed.input.")
	if err := addHealthcheckOptions(f, k, "healthcheck."); err != nil {
		return err
	}
	f.Bool("metrics", false, "enable metrics tracking")
	addMetricsServerOptions(f, "metrics-server.")
	addLogOptions(f, "log.")
	f.Bool("pprof-enable", false, "enable profiling server")

	err := f.Parse(args)
	if err != nil {
		return err
	}

	if f.NArg() != 0 {
		// Unexpected number of parameters
		return errors.New("unexpected number of parameters")
	}

	// Initial application of command line parameters and environment variables so other methods can be applied
	return applyOverrides(f, k)
}

func applyOverrides(f *flag.FlagSet, k *koanf.Koanf) error {
	// Apply command line options and environment variables
	if err := applyOverrideOverrides(f, k); err != nil {
		return err
	}

	// Load configuration file from S3 if setup
	if len(k.String("conf.s3.secret-key")) != 0 {
		if err := loadS3Variables(k, "config.s3."); err != nil {
			return errors.Wrap(err, "error loading S3 settings")
		}

		if err := applyOverrideOverrides(f, k); err != nil {
			return err
		}
	}

	// Local config file overrides S3 config file
	configFile := k.String("conf.file")
	if len(configFile) > 0 {
		if err := k.Load(file.Provider(configFile), json.Parser()); err != nil {
			return errors.Wrap(err, "error loading local config file")
		}

		if err := applyOverrideOverrides(f, k); err != nil {
			return err
		}
	}

	return nil
}

// applyOverrideOverrides for configuration values that need to be re-applied for each configuration item applied
func applyOverrideOverrides(f *flag.FlagSet, k *koanf.Koanf) error {
	// Command line overrides config file or config string
	if err := k.Load(posflag.Provider(f, ".", k), nil); err != nil {
		return errors.Wrap(err, "error loading command line config")
	}

	// Config string overrides any config file
	configString := k.String("conf.string")
	if len(configString) > 0 {
		if err := k.Load(rawbytes.Provider([]byte(configString)), json.Parser()); err != nil {
			return errors.Wrap(err, "error loading config string config")
		}

		// Command line overrides config file or config string
		if err := k.Load(posflag.Provider(f, ".", k), nil); err != nil {
			return errors.Wrap(err, "error loading command line config")
		}
	}

	// Environment variables overrides config files or command line options
	if err := loadEnvironmentVariables(k); err != nil {
		return errors.Wrap(err, "error loading environment variables")
	}

	return nil
}

func loadEnvironmentVariables(k *koanf.Koanf) error {
	envPrefix := k.String("conf.env-prefix")
	if len(envPrefix) != 0 {
		return k.Load(env.Provider(envPrefix+"_", ".", func(s string) string {
			// FOO__BAR -> foo-bar to handle dash in config names
			s = strings.Replace(strings.ToLower(
				strings.TrimPrefix(s, envPrefix+"_")), "__", "-", -1)
			return strings.Replace(s, "_", ".", -1)
		}), nil)
	}

	return nil
}

func endCommonParse(k *koanf.Koanf) (*Config, *Wallet, error) {
	var out Config
	decoderConfig := mapstructure.DecoderConfig{
		ErrorUnused: true,

		// Default values
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc()),
		Metadata:         nil,
		Result:           &out,
		WeaklyTypedInput: true,
	}
	err := k.UnmarshalWithConf("", &out, koanf.UnmarshalConf{DecoderConfig: &decoderConfig})
	if err != nil {

		return nil, nil, err
	}

	if out.Conf.Dump {
		// Print out current configuration

		// Don't keep printing configuration file and don't print wallet password
		err := k.Load(confmap.Provider(map[string]interface{}{
			"conf.dump":       false,
			"wallet.password": "",
		}, "."), nil)

		c, err := k.Marshal(json.Parser())
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to marshal config file to JSON")
		}

		fmt.Println(string(c))
		os.Exit(1)
	}

	// Don't pass around password with normal configuration
	wallet := out.Wallet
	out.Wallet.Password = ""

	return &out, &wallet, nil
}
