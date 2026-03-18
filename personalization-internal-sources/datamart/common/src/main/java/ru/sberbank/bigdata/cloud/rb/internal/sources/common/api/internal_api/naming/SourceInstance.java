package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.clickstream.postfixes.ClickstreamKafkaDaily;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.clickstream.postfixes.ClickstreamNsi;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.DimDzo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.ScdUuid;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.eapteka.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.okko.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.samokat.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbermobail.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbmgmrkt.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbmkt.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sberzvuk.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.saving_certificate.postfixes.SavingCertificateArchive;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.smartvista.postfixes.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.stoplist.postfixes.StoplistNsiDaily;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias.*;

public class SourceInstance {

    @VisibleForTesting
    public static final Map<String, List<SourcePostfix>> INSTANCES_FOR_SOURCE = new HashMap<>();

    //WAY4
    static {
        add(CUSTOM_RB_CARD, new Way4TxnDaily(),
                new Way4TxnMonthly(),
                new Way4Merchants(),
                new Way4DMPymntOptionDaily(),
                new Way4DMAcctStateMonthly(),
                new Way4Nsi(),
                new Way4CardDaily(),
                new Way4CurrencyDaily(),
                new Way4CardAgrmntDaily(),
                new Way4CardAgrmntMonthly(),
                new Way4CredLimitHistDaily(),
                new Way4GlTraceRb(),
                new Way4TxnUpdDaily(),
                new Way4ScdCardPprb(),
                new Way4FtTxnPprbRepl(),
                new Way4UnionTxnPprb(),
                new Way4MerchWay4Pprb(),
                new Way4ScdCardArgmntPprb(),
                new Way4RefCardFirstTransact(),
                new Way4ScdCardUnion(),
                new Way4PprbAgrmntMonthly(),
                new Way4ScdCardAgrmntUnion(),
                new Way4ScdCardReplPprb(),
                new Way4ScdCardAgrmntReplPprb(),
                new Way4MerchPprb(),
                new Way4DimPromoPackage(),
                new Way4DimPromoTariffOverrides(),
                new Way4DimPromoParticipationLimits(),
                new Way4DimMerchants(),
                new Way4MerchWay4Full()
        );
    }

    //ERIB
    static {
        add(CUSTOM_RB_SBOL, EribDailyByInstance.values(),
                EribAhd.values(),
                EribOperAhd.values(),
                EribOper.values(),
                EribOperUpd.values(),
                CatExpensesMonthly.values(),
                AfterAggrFundDaily.values(),
                EribLogonDaily.values(),
                FormType.values(),
                SavGoal.values()
        );

        add(CUSTOM_RB_SBOL, new DailySourcePostfix(),
                new LogonAggrMonthly(),
                new LogonHistDaily(),
                new OperHistDaily(),
                new OperAggrMonthly(),
                new EribSbolConnectors(),
                new OperAggrMonthly(),
                new EribAccountLinks(),
                new SbolLogonLauncher()
        );
    }

    //COD
    static {
        add(CUSTOM_RB_COD,
                IdStaging.values(),
                DepAgrmntSalary.values(),
                CodTariffDaily.values(),
                DepAgrmntDaily.values(),
                CodPaymentDaily.values(),
                CodTxnDaily.values(),
                DepAgrmntState.values(),
                //DepAgrmntAggrPreInit.values(),
                DepAgrmntAggr.values(),
                DepTxnAggr.values(),
                DepAgrmntDailyUpd.values()
                );
        //CodTestingByInstance.values());

        add(CUSTOM_RB_COD, new AggrSalary(),
                new CodNsi(),
                new PprbTariffDaily(),
                new FtDepTxnAggrUnion(),
                new TariffUnion(),
                new DepAgrmntUnion(),
                new DepAgrmntStateUnion(),
                new DepTxnUnion()
                //new CodTesting()
        );
    }

    //CLICKSTREAM
    static {
        add(CUSTOM_RB_CLICKSTREAM, new DailySourcePostfix(),
                new MonthlySourcePostfix(),
                new ClickstreamKafkaDaily(),
                new ClickstreamNsi());
    }

    //EKP
    static {
        add(CUSTOM_RB_LOAN, new EkpAgrmntBalDaily(),
                new EkpCurrencyRateDaily(),
                new EkpLoanAgrmntDaily(),
                new EkpLoanProductDaily(),
                new EkpOperDaily(),
                new EkpOperUpdDaily(),
                new EkpTestReplicaRecovery(),
                new EpkAdditionalEpkDaily(),
                new EkpCredDepositDaily(),
                new EkpLoanMovedDtDaily());
    }

    //SMART_VISTA
    static {
        add(CUSTOM_RB_SMARTVISTA, new SmartvistaArchive(),
                new DailySourcePostfix(),
                new SmartvistaNrtDaily(),
                new SmartvistaTpsDaily(),
                new SmartvistaAggNonClients(),
                new SmartvistaAggNonClientsPriorKafka());
    }

    //INFOBANK
    static {
        add(CUSTOM_RB_SAVING_CERTIFICATE, new DailySourcePostfix(),
                new SavingCertificateArchive());
    }

    //RbTriggers
    static {
        add(CUSTOM_RB_TRIGGERS, new DailySourcePostfix());
    }

    //RiskTriggers
    static {
        add(CUSTOM_RISK_TRIG_INPUT, new DailySourcePostfix());
    }

    //ScoringPlatform
    static {
        add(CUSTOM_RB_SCORING_PLATFORM, new DailySourcePostfix());
    }

    //SPOOBK
    static {
        add(CUSTOM_RB_SPOOBK, new DailySourcePostfix());
    }

    //Stoplist
    static {
        add(CUSTOM_RB_STOPLIST, new DailySourcePostfix(),
                new StoplistNsiDaily());
    }

    //YandexMarket
    static {
        add(CUSTOM_RB_YANDEXMARKET, new DailySourcePostfix());
    }

    //TCURS
    static {
        add(CUSTOM_RB_KURSCUR, new DailySourcePostfix());
    }

    //RoznClientAggr
    static {
        add(CUSTOM_ROZN_CLIENT_AGGR, new MonthlySourcePostfix());
    }

    //EVO

    //Greenplum
    static {
        add(CUSTOM_RB_GREENPLUM,
                new GreenplumEkpDicProduct(),
                new GreenplumEvoRespClnMonthMover(),
                new GreenplumEvoRespClnMonth(),
                new GreenplumEvoPrdctRsltSaltRegular(),
                new GreenplumEvoPrdctRsltSaltRegularMover(),
                new GreenplumEvoRespRules(),
                new GreenplumEvoRespRulesMover(),
                new GreenplumEvoRespClnMover(),
                new GreenplumEvoRespCln(),
                new GreenplumEvoTrxScgRules(),
                new GreenplumEvoTrxScgRulesMover(),
                new GreenplumEvoRespDetail(),
                new GreenplumEvoRespDetailMover(),
                new GreenplumEvoUniverseTables(),
                new GreenplumEvoUniverseTablesMover(),
                new GreenplumEvoTrxMechRespRules(),
                new GreenplumEvoTrxMechRespRulesMover(),
                new GreenplumEvoPrdctRsltSaltMnth(),
                new GreenplumEvoPrdctRsltSaltMnthMover(),
                new GreenplumEvoMonthlyResults(),
                new GreenplumEvoMonthlyResultsMover(),
                new GreenplumEvoExtraSourceRules(),
                new GreenplumEvoExtraSourceRulesMover(),
                new GreenplumEvoEcosystemRespRules(),
                new GreenplumEvoEcosystemRespRulesMover(),
                new GreenplumEvoDicSk(),
                new GreenplumEvoDicSkMover(),
                new GreenplumEvoDic(),
                new GreenplumEvoDicMover(),
                new GreenplumEvoCamp2Rules(),
                new GreenplumEvoCamp2RulesMover(),
                new GreenplumEvoAggResults(),
                new GreenplumEvoAggResultsMover(),
                new GreenplumEvoDicProduct(),
                new GreenplumEvoDicProductMover(),
                new GreenplumEvoKpi(),
                new GreenplumEvoKpiMover(),
                new GreenplumDicAgreement(),
                new GreenplumDicAgreementMover(),
                new GreenplumDicMarketType(),
                new GreenplumDicMarketTypeMover(),
                new GreenplumDicTransType(),
                new GreenplumDicTransTypeMover(),
                new GreenplumBoAgrmnt(),
                new GreenplumBoAgrmntMover(),
                new GreenplumBoSecurityListFull(),
                new GreenplumBoSecurityListFullMover(),
                new GreenplumDicSecurityList(),
                new GreenplumDicSecurityListMover(),
                new GreenplumBoRiskProfile(),
                new GreenplumBoRiskProfileMover(),
                new GreenplumBoAgrmntAttr(),
                new GreenplumBoAgrmntAttrMover(),
                new GreenplumBoAssetBal(),
                new GreenplumBoAssetBalMover(),
                new GreenplumBoJeBalance(),
                new GreenplumBoJeBalanceMover(),
                new GreenplumBoJeDetails(),
                new GreenplumBoJeDetailsMover(),
                new GreenplumBoJeSecurity(),
                new GreenplumBoJeSecurityMover(),
                new GreenplumBoBalance(),
                new GreenplumBoBalanceMover(),
                new GreenplumProductSales(),
                new GreenplumProductSalesMover(),
                new GreenplumSaleChannel(),
                new GreenplumSaleChannelMover(),
                new GreenplumProductClass(),
                new GreenplumProductClassMover(),
                new GreenplumSaleType(),
                new GreenplumSaleTypeMover(),
                new GreenplumVPlEpk(),
                new GreenplumVPlEpkMover(),
                new GreenplumTkBbDaily(),
                new GreenplumTkBbMover(),
                new GreenplumScoringDaily(),
                new GreenplumScoringMover(),
                new GreenplumEvkCampaignDicDaily(),
                new GreenplumEvkCampaignDicMover(),
                new GreenplumEvkCampaignDicSnpMonthly(),
                new GreenplumEvkCampaignChannelSnpMonthly(),
                new GreenplumEvkCampaignChnDaily(),
                new GreenplumEvkCampaignChnMover(),
                new GreenplumEvkCampaignHstDaily(),
                new GreenplumEvkCampaignHstMover(),
                new GreenplumEvkCampaignHist1(),
                new GreenplumEvkCampaignHist1Mover(),
                new GreenplumEvkCampaignHist2(),
                new GreenplumEvkCampaignHist2Mover(),
                new GreenplumEvkCampaignHstSnpMonthly(),
                new GreenplumScoringEpkTopUpDaily(),
                new GreenplumScoringEpkTopUpMover(),
                new GreenplumDicTariff(),
                new GreenplumDicTariffMover(),
                new GreenplumDepFinBillingLocalUfoDaily(),
                new GreenplumDepFinBillingLocalUfoMover(),
                new GreenplumEcosysClientAggDay(),
                new GreenplumEcosysClientAggDayMover(),
                new GreenplumParentFeaturesChildren(),
                new GreenplumParentFeaturesFlags(),
                new GreenplumParentFeaturesChildrenMover(),
                new GreenplumParentFeaturesFlagsMover(),
                new GreenplumDimEcosysDzoClientAggDay(),
                new GreenplumDimEcosysDzoClientAggDayMover(),
                new GreenplumFtEcosysTxnDet(),
                new GreenplumFtEcosysTxnDetMover(),
                new GreenplumScdDzoClientAggParam(),
                new GreenplumEvkFlagFlatAct(),
                new GreenplumEvkFlagFlatActMover(),
                new GreenplumRepCommFunnelNavi(),
                new GreenplumRepCommFunnelNaviMover(),
                new GreenplumEvoKpiScg(),
                new GreenplumEvoKpiScgMover()
        );
    }

    //VNV
    static {
        add(CUSTOM_RB_VNV, new DailySourcePostfix());
    }

    //Scoring
    static {
        add(CUSTOM_RB_SCORING, new DailySourcePostfix());
    }

    //EVK
    static {
        add(CUSTOM_RB_EVK, new DailySourcePostfix());
    }

    //Depfin
    static {
        add(CUSTOM_RB_DEPFIN, new DailySourcePostfix());
    }

    //BO
    static {
        add(CUSTOM_RB_BO, new DailySourcePostfix());
    }

    //EVO

    static {
        add(CUSTOM_ROZN_EVO, new DailySourcePostfix());
    }
    static {
        add(CUSTOM_ROZN_FAMILY, new DailySourcePostfix());
    }

    // EIKP
    static {
        add(CUSTOM_ROZN_EIKP,
                new DimDzo(),
                new DimCampaignOkko(),
                new DimChannelOkko(),
                new DimTargetOkko(),
                new DimActionOkko(),
                new ContactOkko(),
                new DimCampaignSbmkt(),
                new DimChannelSbmkt(),
                new DimTargetSbmkt(),
                new DimActionSbmkt(),
                new ContactSbmkt(),
                new DimCampaignEapteka(),
                new DimChannelEapteka(),
                new DimTargetEapteka(),
                new DimActionEapteka(),
                new ContactEapteka(),
                new ScdUuid(),
                new DimCampaignSamokat(),
                new DimChannelSamokat(),
                new DimTargetSamokat(),
                new DimActionSamokat(),
                new ContactSamokat(),
                new DimActionSbmgmrkt(),
                new ContactSbmgmrkt(),
                new DimTargetSbmgmrkt(),
                new DimCampaignSbmgmrkt(),
                new DimChannelSbmgmrkt(),
                new DimCampaignSbermobail(),
                new DimChannelSbermobail(),
                new DimTargetSbermobail(),
                new DimActionSbermobail(),
                new ContactSbermobail(),
                new DimCampaignSberzvuk(),
                new DimChannelSberzvuk(),
                new DimTargetSberzvuk(),
                new DimActionSberzvuk(),
                new ContactSberzvuk()
                );
    }

    private final String ctlParamName;
    private final String workflowPathName;
    private final String ctlWorkflowName;
    private final String value;

    public SourceInstance(String ctlParamName, String workflowPathName, String ctlWorkflowName, String value) {
        this.ctlParamName = ctlParamName;
        this.workflowPathName = workflowPathName;
        this.ctlWorkflowName = ctlWorkflowName;
        this.value = value.toLowerCase();
    }

    private static void add(SchemaAlias schema, SourcePostfix... sourcePostfixes) {
        INSTANCES_FOR_SOURCE.computeIfAbsent(schema.schemaAliasValue, k -> new ArrayList<>()).addAll(Arrays.asList(sourcePostfixes));
    }

    private static void add(SchemaAlias schema, SourcePostfix[]... sourcePostfixes) {
        add(schema, Stream.of(sourcePostfixes).flatMap(Arrays::stream).toArray(SourcePostfix[]::new));
    }

    public static SourceInstance getSourceInstance(SourcePostfix schemaPostfix) {
        return new SourceInstance("instanceName", schemaPostfix.getPath(), schemaPostfix.getCtlName(), schemaPostfix.getPostfix());
    }

    public static List<SourceInstance> resolveInstancesForSchema(String schemaAlias) {
        if (INSTANCES_FOR_SOURCE.containsKey(schemaAlias)) {
            final List<SourcePostfix> sourcePostfixes = INSTANCES_FOR_SOURCE.get(schemaAlias);
            return sourcePostfixes.stream()
                    .map(SourceInstance::getSourceInstance)
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public String getCtlParamName() {
        return ctlParamName;
    }

    public String getWorkflowPathName() {
        return workflowPathName;
    }

    public String getCtlWorkflowName() {
        return ctlWorkflowName;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceInstance that = (SourceInstance) o;
        return Objects.equals(ctlParamName, that.ctlParamName) &&
                Objects.equals(workflowPathName, that.workflowPathName) &&
                Objects.equals(ctlWorkflowName, that.ctlWorkflowName) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctlParamName, workflowPathName, ctlWorkflowName, value);
    }

    @Override
    public String toString() {
        return "SourceInstance{" +
                "ctlParamName='" + ctlParamName + '\'' +
                ", workflowPathName='" + workflowPathName + '\'' +
                ", ctlWorkflowName='" + ctlWorkflowName + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
