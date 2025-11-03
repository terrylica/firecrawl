import { processRawBranding } from "./processor";
import { BrandingProfile } from "../../types/branding";
import { enhanceBrandingWithLLM } from "./llm";
import { Meta } from "../../scraper/scrapeURL";
import { Document } from "../../controllers/v2/types";
import { BrandingScriptReturn, ButtonSnapshot } from "./types";
import { mergeBrandingResults } from "./merge";

export async function brandingTransformer(
  meta: Meta,
  document: Document,
  rawBranding: BrandingScriptReturn,
): Promise<BrandingProfile> {
  let jsBranding = processRawBranding(rawBranding);

  if (!jsBranding) {
    return {};
  }

  let brandingProfile: BrandingProfile = jsBranding;

  try {
    meta.logger.info("Enhancing branding with LLM...");

    const buttonSnapshots: ButtonSnapshot[] =
      (jsBranding as any).__button_snapshots || [];

    meta.logger.info(
      `Sending ${buttonSnapshots.length} buttons to LLM for classification`,
    );

    const llmEnhancement = await enhanceBrandingWithLLM({
      jsAnalysis: jsBranding,
      buttons: buttonSnapshots,
      screenshot: document.screenshot,
      url: document.url || meta.url,
    });

    meta.logger.info("LLM enhancement complete", {
      primary_btn_index: llmEnhancement.buttonClassification.primaryButtonIndex,
      secondary_btn_index:
        llmEnhancement.buttonClassification.secondaryButtonIndex,
      button_confidence: llmEnhancement.buttonClassification.confidence,
      color_confidence: llmEnhancement.colorRoles.confidence,
    });

    const brandingProfile = mergeBrandingResults(
      jsBranding,
      llmEnhancement,
      buttonSnapshots,
    );

    const DEBUG = process.env.DEBUG_BRANDING === "true";

    if (DEBUG) {
      (brandingProfile as any).__button_snapshots = buttonSnapshots;
    }

    delete (brandingProfile as any).__framework_hints;
    delete (brandingProfile as any).confidence;

    if (!DEBUG) {
      delete (brandingProfile as any).__llm_button_reasoning;
    }
  } catch (error) {
    meta.logger.error(
      "LLM branding enhancement failed, using JS analysis only",
      { error },
    );
    brandingProfile = jsBranding;
    delete (brandingProfile as any).__framework_hints;
  }

  return brandingProfile;
}
