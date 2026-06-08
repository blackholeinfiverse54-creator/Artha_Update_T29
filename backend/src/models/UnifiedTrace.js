import mongoose from 'mongoose';
import { randomUUID } from 'crypto';

/**
 * UnifiedTrace — Single trace system for end-to-end continuity
 * Replaces fragmented trace systems with unified correlation
 * 
 * Design: Every transaction gets ONE trace_id that flows through:
 * - Journal Entry creation
 * - Signal generation
 * - Filing generation
 * - SETU dispatch
 */
const unifiedTraceSchema = new mongoose.Schema({
  trace_id: {
    type: String,
    unique: true,
    required: true,
    immutable: true,
    index: true,
    // Format: TRC-YYYYMMDD-UUID (unified format)
  },
  source: {
    type: String,
    required: true,
    enum: ['INVOICE', 'EXPENSE', 'TDS', 'MANUAL_JOURNAL', 'GST_FILING', 'TDS_FILING'],
  },
  source_id: {
    type: String,
    required: true,
    // MongoDB ObjectId as string (invoice._id, expense._id, etc.)
  },
  initiated_by: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
  initiated_at: {
    type: Date,
    default: Date.now,
    immutable: true,
  },
  
  // Lifecycle tracking
  stages: [{
    stage: {
      type: String,
      enum: ['TRANSACTION_CREATED', 'JOURNAL_CREATED', 'JOURNAL_VALIDATED', 'JOURNAL_POSTED', 
             'SIGNAL_GENERATED', 'FILING_CREATED', 'FILING_VALIDATED', 'SETU_DISPATCHED'],
      required: true,
    },
    entity_type: {
      type: String,
      enum: ['Invoice', 'Expense', 'TDSEntry', 'JournalEntry', 'ComplianceSignal', 
             'ComplianceFiling', 'ComplianceValidationLog', 'SetuDispatch'],
    },
    entity_id: {
      type: String,
      required: true,
    },
    status: {
      type: String,
      enum: ['SUCCESS', 'FAILED', 'PENDING', 'SKIPPED'],
      required: true,
    },
    timestamp: {
      type: Date,
      default: Date.now,
    },
    metadata: mongoose.Schema.Types.Mixed,
    error: String,
  }],
  
  // Current state
  current_stage: {
    type: String,
    enum: ['TRANSACTION_CREATED', 'JOURNAL_CREATED', 'JOURNAL_VALIDATED', 'JOURNAL_POSTED', 
           'SIGNAL_GENERATED', 'FILING_CREATED', 'FILING_VALIDATED', 'SETU_DISPATCHED', 'FAILED'],
    default: 'TRANSACTION_CREATED',
  },
  status: {
    type: String,
    enum: ['IN_PROGRESS', 'COMPLETED', 'FAILED'],
    default: 'IN_PROGRESS',
  },
  
  // Linked entities (denormalized for fast lookup)
  linked_entities: {
    journal_entries: [{ type: mongoose.Schema.Types.ObjectId, ref: 'JournalEntry' }],
    signals: [{ type: mongoose.Schema.Types.ObjectId, ref: 'ComplianceSignal' }],
    filings: [{ type: mongoose.Schema.Types.ObjectId, ref: 'ComplianceFiling' }],
    validation_logs: [{ type: mongoose.Schema.Types.ObjectId, ref: 'ComplianceValidationLog' }],
    setu_dispatches: [String], // SETU dispatch IDs
  },
  
  // Replay capability
  replay_available: {
    type: Boolean,
    default: true,
  },
  replay_count: {
    type: Number,
    default: 0,
  },
  last_replayed_at: Date,
  
  // Causality chain
  caused_by: {
    type: String,
    ref: 'UnifiedTrace',
    // Reference to parent trace_id if this trace was triggered by another
  },
  triggers: [{
    type: String,
    ref: 'UnifiedTrace',
    // Child trace_ids triggered by this trace
  }],
  
}, {
  timestamps: true,
});

// Indexes for performance
unifiedTraceSchema.index({ source: 1, source_id: 1 });
unifiedTraceSchema.index({ initiated_by: 1, initiated_at: -1 });
unifiedTraceSchema.index({ current_stage: 1, status: 1 });
unifiedTraceSchema.index({ 'stages.stage': 1, 'stages.timestamp': -1 });
unifiedTraceSchema.index({ 'linked_entities.journal_entries': 1 });
unifiedTraceSchema.index({ 'linked_entities.signals': 1 });

// Generate unified trace_id
unifiedTraceSchema.statics.generateTraceId = function() {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const uuid = randomUUID().slice(0, 8);
  return `TRC-${date}-${uuid}`;
};

// Add stage to trace
unifiedTraceSchema.methods.addStage = async function(stageData) {
  this.stages.push({
    stage: stageData.stage,
    entity_type: stageData.entity_type,
    entity_id: stageData.entity_id,
    status: stageData.status || 'SUCCESS',
    timestamp: new Date(),
    metadata: stageData.metadata,
    error: stageData.error,
  });
  
  if (stageData.status === 'SUCCESS') {
    this.current_stage = stageData.stage;
  } else if (stageData.status === 'FAILED') {
    this.status = 'FAILED';
    this.current_stage = 'FAILED';
  }
  
  // Update linked entities
  if (stageData.entity_type && stageData.entity_id) {
    const entityMap = {
      'JournalEntry': 'journal_entries',
      'ComplianceSignal': 'signals',
      'ComplianceFiling': 'filings',
      'ComplianceValidationLog': 'validation_logs',
    };
    
    const field = entityMap[stageData.entity_type];
    if (field && this.linked_entities[field]) {
      this.linked_entities[field].push(stageData.entity_id);
    }
  }
  
  await this.save();
  return this;
};

// Get full trace chain with all linked entities
unifiedTraceSchema.methods.getFullChain = async function() {
  await this.populate([
    { path: 'linked_entities.journal_entries', select: 'entryNumber date status hash chainPosition' },
    { path: 'linked_entities.signals', select: 'signal_id type severity created_at' },
    { path: 'linked_entities.filings', select: 'filingId filingType created_at' },
    { path: 'linked_entities.validation_logs', select: 'filingId filing_ready errors' },
    { path: 'initiated_by', select: 'name email' },
  ]);
  
  return this;
};

export default mongoose.model('UnifiedTrace', unifiedTraceSchema);
