import discord
from discord.ext import commands
import psycopg2
import asyncio
import requests
import time
import os

print("this is a test change")

# --------------------------------------------------------------------
# Database connection (Postgres)
# --------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is missing! Make sure it's set in Railway.")

# Create a connection + cursor
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Ensure the Users table exists
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Users (
        DiscordID TEXT PRIMARY KEY,
        RobloxID TEXT,
        r_user TEXT,
        EventsAttended INTEGER DEFAULT 0,
        EventsHosted INTEGER DEFAULT 0,
        FlightMinutes INTEGER DEFAULT 0,
        QuotaMet BOOLEAN DEFAULT FALSE,
        Rank TEXT DEFAULT 'Unknown'
    );
    """
)
conn.commit()

# Add columns for inactivity if they don't exist
cursor.execute(
    """
    ALTER TABLE Users
    ADD COLUMN IF NOT EXISTS Inactive BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS InactiveStart DATE,
    ADD COLUMN IF NOT EXISTS InactiveEnd DATE,
    ADD COLUMN IF NOT EXISTS InactiveReason TEXT;
    """
)
conn.commit()

# Add Strikes column (integer) if it doesn't exist
cursor.execute(
    """
    ALTER TABLE Users
    ADD COLUMN IF NOT EXISTS Strikes INTEGER DEFAULT 0;
    """
)
conn.commit()

# Add ImmuneRoleStart column to track when immune role was granted
cursor.execute(
    """
    ALTER TABLE Users
    ADD COLUMN IF NOT EXISTS ImmuneRoleStart DATE;
    """
)
conn.commit()

# --------------------------------------------------------------------
# Bot Setup
# --------------------------------------------------------------------
intents = discord.Intents.default()
intents.members = True  # needed to see all members in larger servers
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

RATE_LIMIT_DELAY = 1  # delay (seconds) between external API calls

# --------------------------------------------------------------------
# Token retrieval
# --------------------------------------------------------------------
BOT_TOKEN = os.getenv("TOKEN")
if not BOT_TOKEN:
    raise ValueError("TOKEN environment variable is missing! Make sure it's set in Railway.")

# --------------------------------------------------------------------
# Roles and IDs
# --------------------------------------------------------------------
allowed_role_ids = {
    830563690901929994,  # Insert your relevant role IDs
    830563690096754709,
    830563689618079814,
    948638117299118170,
    830563688918155314,
    1087425984585805834,
    1429947897377718332,
}

qualifying_role_ids = [
    830563693808844850,
    830563693121110016,
    830563692264554496,
    830563691173773413,
    830563690901929994,
    830563690096754709,
    830563689618079814,
    1429947897377718332,
    948638117299118170,
]

# Required role to use certain commands
REQUIRED_ROLE_ID_FOR_OTHERS = 1429947897377718332

# Channel IDs
REVIEW_CHANNEL_ID = 897184372027969576
INACTIVITY_APPROVAL_CHANNEL_ID = 897184372027969576  # Replace if you have a separate channel

# Flight logs pending approval
pending_flight_logs = {}  # message_id -> { user_id, minutes, origin_channel_id }

# Inactivity requests pending approval
pending_inactivity_requests = {}  # message_id -> { user_id, start_date, end_date, reason }

# Exempt roles from strikes (i.e. do not penalize them in !enforce_quota)
exempt_role_ids = {
    830563689618079814,  # e.g. "HC"
    948638117299118170,  # example
    830563688918155314,  # example
    897190724297195580,  # example
    830563688179826738,  # example
    914638548731322419, #crimson squad
    830563693808844850,  # immune role
}

# Immune role ID for tracking
IMMUNE_ROLE_ID = 830563693808844850

# --------------------------------------------------------------------
# Permission helpers
# --------------------------------------------------------------------
def user_has_any_allowed_role(member: discord.Member) -> bool:
    """Check if the member has at least one role from 'allowed_role_ids' (for !log_event)."""
    return any(role.id in allowed_role_ids for role in member.roles)

def has_qualifying_role(member: discord.Member):
    """Check if the member has any role from 'qualifying_role_ids'."""
    return any(role.id in qualifying_role_ids for role in member.roles)

def get_highest_qualifying_role(member: discord.Member, guild: discord.Guild):
    """Get the highest role from 'qualifying_role_ids' that the member has, by role position."""
    member_roles = [r for r in member.roles if r.id in qualifying_role_ids]
    if not member_roles:
        return None
    # Sort by descending role position
    member_roles.sort(key=lambda r: r.position, reverse=True)
    return member_roles[0].name

# --------------------------------------------------------------------
# Roblox + RoVer Helpers
# --------------------------------------------------------------------
def fetch_latest_roblox_username(roblox_id):
    """
    Fetch the current username for a given Roblox ID.
    If the ID is invalid or the request fails, return "Unknown".
    """
    url = f"https://users.roblox.com/v1/users/{roblox_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get("name", "Unknown")
        else:
            print(f"Error fetching username for ROBLOX ID {roblox_id}: {response.text}")
            return "Unknown"
    except requests.exceptions.RequestException as e:
        print(f"Error fetching latest username: {e}")
        return "Unknown"

def fetch_roblox_id_from_rover(discord_id, guild_id):
    """
    Attempt to fetch a real Roblox ID from RoVer. If it fails, returns (None, None).
    """
    ROVER_API_KEY = "YOUR_ROVER_API_KEY_HERE"  # Replace with your real key or use an env var.
    url = f"https://registry.rover.link/api/guilds/{guild_id}/discord-to-roblox/{discord_id}"
    headers = {"Authorization": f"Bearer {ROVER_API_KEY}"}

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 1.0))
            time.sleep(retry_after)
            return fetch_roblox_id_from_rover(discord_id, guild_id)

        if response.status_code == 404:
            return None, None

        response.raise_for_status()
        data = response.json()
        if "robloxId" in data:
            roblox_id = str(data["robloxId"])
            latest_username = fetch_latest_roblox_username(roblox_id)
            return roblox_id, latest_username
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching ROBLOX ID from RoVer: {e}")
        return None, None

# --------------------------------------------------------------------
# Helper: Ensure a User Record
# --------------------------------------------------------------------
def ensure_user_record(member: discord.Member, guild: discord.Guild):
    """
    Ensure the user is in the database with a valid RobloxID or fallback "DISCORD-<discord_id>".
    Returns (roblox_id, roblox_username).
    """
    discord_id_str = str(member.id)

    cursor.execute("SELECT RobloxID, r_user FROM Users WHERE DiscordID=%s", (discord_id_str,))
    row = cursor.fetchone()

    if row:
        existing_roblox_id, existing_user_name = row
        if existing_roblox_id:
            # Already have a Roblox ID or fallback; update their rank if needed
            rank = get_highest_qualifying_role(member, guild) or "Unknown"
            cursor.execute("UPDATE Users SET Rank=%s WHERE DiscordID=%s", (rank, discord_id_str))
            
            # Check if user has immune role and track start date
            has_immune_role = any(role.id == IMMUNE_ROLE_ID for role in member.roles)
            cursor.execute("SELECT ImmuneRoleStart FROM Users WHERE DiscordID=%s", (discord_id_str,))
            immune_start = cursor.fetchone()[0] if cursor.rowcount > 0 else None
            
            if has_immune_role and immune_start is None:
                # User got immune role, record the date
                cursor.execute("UPDATE Users SET ImmuneRoleStart=CURRENT_DATE WHERE DiscordID=%s", (discord_id_str,))
            elif not has_immune_role and immune_start is not None:
                # User lost immune role, clear the date
                cursor.execute("UPDATE Users SET ImmuneRoleStart=NULL WHERE DiscordID=%s", (discord_id_str,))
            
            conn.commit()
            return existing_roblox_id, existing_user_name
        else:
            # No RobloxID => try to fetch from RoVer, else fallback
            fetched_id, fetched_name = fetch_roblox_id_from_rover(discord_id_str, guild.id)
            if not fetched_id:
                fetched_id = f"DISCORD-{discord_id_str}"
                fetched_name = member.display_name

            rank = get_highest_qualifying_role(member, guild) or "Unknown"
            cursor.execute(
                "UPDATE Users SET RobloxID=%s, r_user=%s, Rank=%s WHERE DiscordID=%s",
                (fetched_id, fetched_name, rank, discord_id_str)
            )
            
            # Check if user has immune role and track start date
            has_immune_role = any(role.id == IMMUNE_ROLE_ID for role in member.roles)
            if has_immune_role:
                cursor.execute("UPDATE Users SET ImmuneRoleStart=CURRENT_DATE WHERE DiscordID=%s", (discord_id_str,))
            
            conn.commit()
            return fetched_id, fetched_name
    else:
        # No row => create one
        fetched_id, fetched_name = fetch_roblox_id_from_rover(discord_id_str, guild.id)
        if not fetched_id:
            fetched_id = f"DISCORD-{discord_id_str}"
            fetched_name = member.display_name

        rank = get_highest_qualifying_role(member, guild) or "Unknown"
        
        # Check if user has immune role
        has_immune_role = any(role.id == IMMUNE_ROLE_ID for role in member.roles)
        immune_start_date = "CURRENT_DATE" if has_immune_role else "NULL"
        
        cursor.execute(
            f"""
            INSERT INTO Users (DiscordID, RobloxID, r_user,
                               EventsAttended, EventsHosted,
                               FlightMinutes, QuotaMet, Rank, Strikes, ImmuneRoleStart)
            VALUES (%s, %s, %s, 0, 0, 0, FALSE, %s, 0, {immune_start_date})
            """,
            (discord_id_str, fetched_id, fetched_name, rank)
        )
        conn.commit()
        return fetched_id, fetched_name

# --------------------------------------------------------------------
# Quota-related
# --------------------------------------------------------------------
def recalculate_quota():
    """
    Recalculate ‘QuotaMet’ for all users in the database:
      - 1 events attended, OR
      - 1 events hosted, OR
      - >= 30 flight minutes
    """
    cursor.execute("SELECT DiscordID, EventsAttended, EventsHosted, FlightMinutes FROM Users")
    rows = cursor.fetchall()

    for (disc_id, attended, hosted, flight_minutes) in rows:
        attended = attended or 0
        hosted = hosted or 0
        flight_minutes = flight_minutes or 0

        meets_quota = (
            (attended >= 1)
            or (hosted >= 1)
            or (flight_minutes >= 30)
        )
        cursor.execute("UPDATE Users SET QuotaMet=%s WHERE DiscordID=%s", (meets_quota, disc_id))

    conn.commit()
    print("Quota recalculated for all users.")

# --------------------------------------------------------------------
# Decorator to restrict commands to a specific role
# --------------------------------------------------------------------
def require_specific_role(role_id):
    def predicate(ctx):
        return any(r.id == role_id for r in ctx.author.roles)
    return commands.check(predicate)

# --------------------------------------------------------------------
# !log_flight (everyone can use)
# --------------------------------------------------------------------
@bot.command(name="log_flight")
async def log_flight(ctx):
    def check_author(m):
        return (m.author == ctx.author) and (m.channel == ctx.channel)

    # Step 1: ask for minutes
    await ctx.send("✈️ How many minutes would you like to log?")
    try:
        minutes_msg = await bot.wait_for("message", timeout=60.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long to respond. Please try again.")
        return
    await ctx.send("💀 How many kills did you gain during your flight?")
    try:
        kills_msg = await bot.wait_for("message", timeout=60.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("You took too long to respond. Please try again.")
        return

    # Validate minutes
    try:
        minutes = int(minutes_msg.content)
        if minutes <= 0:
            await ctx.send("❌ Minutes must be a positive integer.")
            return
    except ValueError:
        await ctx.send("❌ Please enter a valid number of minutes.")
        return
    
    # Validate kills
    try:
        kills = int(kills_msg.content)
        if kills <= 0:
            await ctx.send("❌ You must have atleast one kill for your flight log to be valid.")
            return 
    except ValueError:
        await ctx.send("❌ Please enter a valid number of kills")
        return

    # Ensure the user is in the DB right away
    ensure_user_record(ctx.author, ctx.guild)

    # Step 2: screenshot prompt
    await ctx.send("📸 Please provide a screenshot of your flight time and kills.")
    try:
        screenshot_msg = await bot.wait_for("message", timeout=120.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long to provide a screenshot. Please try again.")
        return

    if not screenshot_msg.attachments:
        await ctx.send("❌ No attachment detected. Please try again and provide a screenshot.")
        return

    attachment_url = screenshot_msg.attachments[0].url
    review_channel = bot.get_channel(REVIEW_CHANNEL_ID)
    if review_channel is None:
        await ctx.send("❌ Could not find the review channel. Contact an administrator.")
        return

    # Send an embed for review
    embed = discord.Embed(title="Flight Log Review", color=discord.Color.blue())
    embed.add_field(name="User", value=ctx.author.display_name, inline=True)
    embed.add_field(name="Minutes", value=str(minutes), inline=True)
    embed.add_field(name="Kills", value=str(kills), inline=True)
    embed.set_image(url=attachment_url)

    review_message = await review_channel.send(embed=embed)
    pending_flight_logs[review_message.id] = {
        "user_id": ctx.author.id,
        "minutes": minutes,
        "kills": kills,
        "origin_channel_id": ctx.channel.id,
    }

    # Add approve/deny reactions
    await review_message.add_reaction("✅")
    await review_message.add_reaction("❌")

    await ctx.send("✅ Your flight log has been submitted for review. Please wait for approval.")

# --------------------------------------------------------------------
# !log_event restricted to roles in allowed_role_ids
# --------------------------------------------------------------------
@bot.command(name="log_event")
async def log_event(ctx):
    if not user_has_any_allowed_role(ctx.author):
        await ctx.send("❌ You do not have permission to use this command.")
        return

    try:
        await ctx.guild.chunk()
    except Exception as e:
        print(f"Warning: Could not chunk guild: {e}")
        # Continue anyway, chunking is not critical

    def check_author(m):
        return (m.author == ctx.author) and (m.channel == ctx.channel)

    # Co-host
    await ctx.send(
        "Please **mention your co-host** (one user) or type **none** if no co-host.\n"
        "You have 60 seconds to respond."
    )
    try:
        cohost_msg = await bot.wait_for("message", timeout=60.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long. Command cancelled.")
        return

    cohost_user = None
    cohost_text = cohost_msg.content.strip().lower()
    if cohost_text == "none":
        cohost_user = None
    elif cohost_msg.mentions:
        cohost_user = cohost_msg.mentions[0]
    else:
        await ctx.send("❌ Could not parse a valid co-host mention. No co-host will be set.")
        cohost_user = None

    # Event name
    await ctx.send("Please **enter the event name** (e.g., 'Basic Flight Training'). You have 60 seconds.")
    try:
        event_name_msg = await bot.wait_for("message", timeout=60.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long. Command cancelled.")
        return

    event_name = event_name_msg.content.strip()
    if not event_name:
        await ctx.send("No event name provided. Command cancelled.")
        return

    # Collect attendees
    attendees_input = []
    await ctx.send(
        "**Enter each attendee one by one** (Mention with @[DISCORD USER]).\n"
        "Type **done** when finished. You have 60 seconds per entry."
    )
    while True:
        try:
            msg = await bot.wait_for("message", timeout=60.0, check=check_author)
        except asyncio.TimeoutError:
            await ctx.send("❌ You took too long to respond. Command cancelled.")
            return

        content = msg.content.strip()
        if content.lower() == "done":
            await ctx.send("✅ Finished collecting attendees.")
            break
        attendees_input.append(msg)
        await ctx.send(f"Got: {content}. Enter another attendee or done...")

    if not attendees_input:
        await ctx.send("No attendees were provided. Command cancelled.")
        return

    # Screenshot proof
    await ctx.send("📸 Please provide a screenshot for proof of the event. You have 120 seconds.")
    try:
        proof_msg = await bot.wait_for("message", timeout=120.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long to provide a screenshot. Command cancelled.")
        return

    if not proof_msg.attachments:
        await ctx.send("❌ No screenshot detected. Command cancelled.")
        return

    screenshot_url = proof_msg.attachments[0].url

    # Update DB for each attendee
    attendee_pings_for_summary = []
    attendees_processed = []

    try:
        for attendee_msg in attendees_input:
            content = attendee_msg.content.strip()
            mention_list = attendee_msg.mentions

            if mention_list:
                # Attendee was a mention
                attendee_member = mention_list[0]
                try:
                    ensure_user_record(attendee_member, ctx.guild)
                    cursor.execute(
                        "UPDATE Users SET EventsAttended=EventsAttended+1 WHERE DiscordID=%s",
                        (str(attendee_member.id),)
                    )
                    conn.commit()

                    attendees_processed.append(str(attendee_member.id))
                    attendee_pings_for_summary.append(attendee_member.mention)
                except Exception as e:
                    await ctx.send(f"❌ Error processing attendee {attendee_member.mention}: {str(e)}")
                    continue

            elif content.isdigit():
                # The input is a numeric Roblox ID
                roblox_id = content
                try:
                    roblox_username = fetch_latest_roblox_username(roblox_id)
                    cursor.execute("SELECT DiscordID FROM Users WHERE RobloxID=%s", (roblox_id,))
                    row = cursor.fetchone()

                    if row:
                        # Already in the DB
                        cursor.execute(
                            "UPDATE Users SET EventsAttended=EventsAttended+1 WHERE RobloxID=%s",
                            (roblox_id,)
                        )
                        conn.commit()
                        attendees_processed.append(str(row[0]))
                    else:
                        # Create new record with DiscordID='0'
                        cursor.execute(
                            """
                            INSERT INTO Users (DiscordID, RobloxID, r_user, EventsAttended, EventsHosted,
                                               FlightMinutes, QuotaMet, Rank, Strikes)
                            VALUES (%s, %s, %s, 1, 0, 0, FALSE, %s, 0)
                            """,
                            ("0", roblox_id, roblox_username, "Unknown")
                        )
                        conn.commit()
                        attendees_processed.append("0")
                    attendee_pings_for_summary.append(f"RobloxID:{roblox_id}")
                except Exception as e:
                    await ctx.send(f"❌ Error processing Roblox ID {roblox_id}: {str(e)}")
                    continue

            else:
                await ctx.send(f"❌ Could not parse attendee: {content}. Skipping.")
                continue

            await asyncio.sleep(RATE_LIMIT_DELAY)

        # Host
        try:
            ensure_user_record(ctx.author, ctx.guild)
            cursor.execute(
                "UPDATE Users SET EventsHosted=EventsHosted+1 WHERE DiscordID=%s",
                (str(ctx.author.id),)
            )
            conn.commit()
        except Exception as e:
            await ctx.send(f"❌ Error updating host record: {str(e)}")
            return

        # Co-host
        cohost_mention_str = "None"
        if cohost_user is not None:
            try:
                cohost_mention_str = cohost_user.mention
                ensure_user_record(cohost_user, ctx.guild)
                cursor.execute(
                    "UPDATE Users SET EventsHosted=EventsHosted+1 WHERE DiscordID=%s",
                    (str(cohost_user.id),)
                )
                conn.commit()
            except Exception as e:
                await ctx.send(f"❌ Error updating co-host record: {str(e)}")
                # Continue anyway since co-host is optional

        # Recalculate quota
        try:
            recalculate_quota()
        except Exception as e:
            await ctx.send(f"⚠️ Warning: Error recalculating quota: {str(e)}")
            # Continue anyway since quota recalculation is not critical for the event log
    except Exception as e:
        await ctx.send(f"❌ Critical error during event logging: {str(e)}")
        return

    # Final summary
    try:
        final_channel = bot.get_channel(830596103434534932)  # or use ctx.channel if you prefer
        if final_channel is None:
            final_channel = ctx.channel

        host_mention = ctx.author.mention
        attendees_str = ", ".join(attendee_pings_for_summary) if attendee_pings_for_summary else "None"

        await final_channel.send("✅ **Event logging complete!**")
        await final_channel.send(
            f"Host: {host_mention}\n"
            f"Co-host: {cohost_mention_str}\n"
            f"Event: {event_name}\n"
            f"Attendees: {attendees_str}\n"
            f"Proof: {screenshot_url}"
        )

        if attendees_processed:
            await final_channel.send(f"**Attendees DB Updated:** {', '.join(attendees_processed)}")
    except Exception as e:
        await ctx.send(f"❌ Error sending final summary: {str(e)}")
        # Still send a basic confirmation to the original channel
        await ctx.send("⚠️ Event was logged to database, but there was an error sending the summary.")
        
        
@bot.command(name="leaderboard")
async def leaderboard(ctx):
    if not user_has_any_allowed_role(ctx.author):
        await ctx.send("❌ You do not have permission to use this command.")
        return
    """
    Displays the top 15 most active pilots
    """
    top_fifteen_pilots = []
    cursor.execute("SELECT discordid, eventsattended FROM users ORDER BY eventsattended DESC LIMIT 15")
    top_fifteen_pilots = cursor.fetchall()
    if not top_fifteen_pilots:
        await ctx.send("No pilots have attended any events yet for this quota cycle.")
        return

    lb_message = "**Top 15 Most Active Pilots**\n"
    for idpilot, (discordid, eventsattended) in enumerate(top_fifteen_pilots, start=1):
        lb_message += f"{idpilot}: <@{discordid}> > {eventsattended} events attended\n"

    await ctx.send(lb_message)
    
@bot.command(name="Officer_LB")
async def Officer_LB(ctx):
    """
    Displays most active Squadron Leaders in order.
    """
    
    top_active_SLs = []
    cursor.execute("SELECT discordid, eventshosted FROM users WHERE rank='Squadron Leader' OR rank='Wing Commander' OR rank='Group Commandant' ORDER BY eventshosted DESC LIMIT 15")
    top_active_SLs = cursor.fetchall()
    if not top_active_SLs:
        await ctx.send("There are no squadron leaders or rear admirals who have hosted this quota cycle")
        return
    
    lb_message = "**Top Active Squadron Leaders**\n"
    for idSL, (discordid, eventshosted) in enumerate(top_active_SLs, start=1):
        lb_message += f"{idSL}: <@{discordid}> > {eventshosted} events hosted\n"

    await ctx.send(lb_message)


@bot.command(name="manual_log")
async def manual_log(ctx, user: discord.Member):
    """
    Usage: !manual_log @Member
    Manually adds 1 event log to a user's record in the database.
    """
    if not user_has_any_allowed_role(ctx.author):
        await ctx.send("❌ You do not have permission to use this command.")
        return

    disc_id_str = str(user.id)
    cursor.execute("SELECT DiscordID FROM Users WHERE DiscordID=%s", (disc_id_str,))
    row = cursor.fetchone()

    if not row:
        await ctx.send(f"No database record found for {user.mention}.")
        return

    cursor.execute(
        """
        UPDATE Users
        SET EventsAttended = EventsAttended + 1
        WHERE DiscordID=%s
        """,
        (disc_id_str,)
    )
    conn.commit()

    await ctx.send(f"✅ 1 event log has been manually added to {user.mention}'s record.")

# --------------------------------------------------------------------
# ALL OTHER COMMANDS restricted to REQUIRED_ROLE_ID_FOR_OTHERS
# --------------------------------------------------------------------
@bot.command(name="register")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def register(ctx, roblox_id: str):
    """
    Let a user manually set their own Roblox ID (overrides fallback).
    """
    discord_id = str(ctx.author.id)
    latest_username = fetch_latest_roblox_username(roblox_id)
    rank = get_highest_qualifying_role(ctx.author, ctx.guild) or "Unknown"

    cursor.execute("SELECT * FROM Users WHERE DiscordID=%s", (discord_id,))
    row = cursor.fetchone()

    if row:
        # Update existing record
        cursor.execute(
            "UPDATE Users SET RobloxID=%s, r_user=%s, Rank=%s WHERE DiscordID=%s",
            (roblox_id, latest_username, rank, discord_id)
        )
        await ctx.send(
            f"Your ROBLOX ID has been updated to {roblox_id} (username {latest_username}), rank {rank}."
        )
    else:
        # Create new record
        cursor.execute(
            """
            INSERT INTO Users (DiscordID, RobloxID, r_user,
                               EventsAttended, EventsHosted,
                               FlightMinutes, QuotaMet, Rank, Strikes)
            VALUES (%s, %s, %s, 0, 0, 0, FALSE, %s, 0)
            """,
            (discord_id, roblox_id, latest_username, rank)
        )
        await ctx.send(
            f"Your ROBLOX ID {roblox_id} (username {latest_username}) rank {rank} has been registered."
        )

    conn.commit()

@bot.command(name="purge_unqualified_users")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def purge_unqualified_users(ctx):
    """
    Removes all users from the database who are still in the Discord server
    but do NOT have a qualifying role.
    """
    cursor.execute("SELECT DiscordID FROM Users")
    all_users = cursor.fetchall()

    removed_count = 0
    await ctx.guild.chunk()

    for (disc_id_str,) in all_users:
        if disc_id_str.isdigit():  # Ensure it's a valid Discord ID
            member = ctx.guild.get_member(int(disc_id_str))
            if member:
                # Check if they have a qualifying role
                if not has_qualifying_role(member):
                    cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
                    conn.commit()
                    removed_count += 1

    if removed_count == 0:
        await ctx.send("✅ No unqualified users were found in the database.")
    else:
        await ctx.send(f"✅ Removed **{removed_count}** users from the database who no longer qualify.")



@bot.command(name="report_quota")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def report_quota(ctx):
    """
    Shows all users who have NOT met their quota, in a nicer format.
    Also recalculates quota first.
    If a user left the server, remove them from DB instead of listing them.
    """
    recalculate_quota()
    cursor.execute("SELECT DiscordID FROM Users WHERE QuotaMet=false AND Inactive=false")
    rows = cursor.fetchall()

    if not rows:
        embed = discord.Embed(
            title="Quota Report",
            description="All users met their quota!",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    lines = []
    removed_count = 0

    for (disc_id_str,) in rows:
        if disc_id_str.isdigit():
            member = ctx.guild.get_member(int(disc_id_str))
            if not member:
                # user left, remove from DB
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
                conn.commit()
                removed_count += 1
            else:
                # Check if the member has an exempt role
                if any(role.id in exempt_role_ids for role in member.roles):
                    continue
                lines.append(f"• {member.mention}")
        else:
            # if DiscordID not numeric, remove
            cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
            conn.commit()
            removed_count += 1

    if not lines:
        embed = discord.Embed(
            title="Quota Report",
            description=f"All non-quota users have left. Removed {removed_count} records.\nNo one left to report!",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    def chunk_string(txt, limit=1800):
        return [txt[i : i + limit] for i in range(0, len(txt), limit)]

    big_text = "\n".join(lines)
    chunks = chunk_string(big_text)
    for i, chunk in enumerate(chunks, start=1):
        embed = discord.Embed(
            title=f"Users who did not meet quota (Page {i}/{len(chunks)})",
            description=chunk,
            color=discord.Color.red()
        )
        if removed_count > 0 and i == 1:
            embed.set_footer(text=f"Removed {removed_count} DB entries for ex-members.")
        await ctx.send(embed=embed)

@bot.command(name="check_quota")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def check_quota(ctx):
    recalculate_quota()
    await ctx.send("Quota recalculated for all users.")

@bot.command(name="add_qualified_members")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def add_qualified_members(ctx):
    """
    For each member in the server who has a qualifying role, ensure they're in the DB
    if they're missing. Does NOT ping or list those already in the DB; only a summary.
    """
    added_count = 0
    await ctx.guild.chunk()

    for member in ctx.guild.members:
        if has_qualifying_role(member):
            disc_id_str = str(member.id)
            cursor.execute("SELECT DiscordID FROM Users WHERE DiscordID=%s", (disc_id_str,))
            row = cursor.fetchone()
            if not row:
                ensure_user_record(member, ctx.guild)
                added_count += 1

    await ctx.send(f"{added_count} members with qualifying roles were ensured in the database.")

@bot.command(name="check_missing_entries")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def check_missing_entries(ctx):
    """
    We consider 'missing' if RobloxID is DISCORD-xx or '0' or empty,
    or if r_user is null/empty. We'll show them in a nice embed.
    """
    query = """
    SELECT DiscordID, RobloxID, r_user
    FROM Users
    WHERE
       (RobloxID IS NULL OR RobloxID='' OR RobloxID LIKE 'DISCORD-%' OR RobloxID='0')
       OR
       (r_user IS NULL OR r_user='')
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        embed = discord.Embed(
            title="Missing Entries",
            description="No fallback/empty entries found.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    lines = []
    for (d_id, rb_id, r_user_val) in rows:
        if d_id.isdigit() and d_id != "0":
            lines.append(f"• <@{d_id}> => RobloxID: {rb_id}, r_user: {r_user_val}")
        else:
            lines.append(f"• DiscordID: {d_id}, RobloxID: {rb_id}, r_user: {r_user_val}")

    def chunk_string(txt, limit=1800):
        return [txt[i : i + limit] for i in range(0, len(txt), limit)]

    big_list_str = "\n".join(lines)
    chunks = chunk_string(big_list_str)
    for i, chunk in enumerate(chunks, start=1):
        embed = discord.Embed(
            title=f"Missing Entries (Page {i}/{len(chunks)})",
            description=chunk,
            color=discord.Color.gold()
        )
        await ctx.send(embed=embed)

@bot.command(name="log_all_members")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def log_all_members(ctx):
    """
    Iterate through all members, ensure they have a record if they have a qualifying role.
    If they have a fallback or no record, update them accordingly.
    """
    await ctx.guild.chunk()
    updated_count = 0

    for member in ctx.guild.members:
        if has_qualifying_role(member):
            disc_id_str = str(member.id)
            cursor.execute("SELECT RobloxID FROM Users WHERE DiscordID=%s", (disc_id_str,))
            old_row = cursor.fetchone()

            roblox_id, roblox_name = ensure_user_record(member, ctx.guild)
            if not old_row or not old_row[0] or old_row[0].startswith("DISCORD-"):
                updated_count += 1
                await ctx.send(
                    f"✅ Ensured {member.mention} => RobloxID: {roblox_id}, Username: {roblox_name}"
                )

            await asyncio.sleep(RATE_LIMIT_DELAY)

    await ctx.send(f"✅ log_all_members completed. {updated_count} records updated or added.")

@bot.command(name="ping_unregistered")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def ping_unregistered(ctx):
    """
    Finds entries in the DB where RobloxID is 'DISCORD-xx' and pings them if they're still in the server.
    If they left, remove them from DB.
    """
    cursor.execute("SELECT DiscordID FROM Users WHERE RobloxID LIKE 'DISCORD-%'")
    rows = cursor.fetchall()

    if not rows:
        embed = discord.Embed(
            title="Unregistered Users",
            description="No fallback/unregistered users found.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    lines = []
    removed_count = 0

    for (d_id,) in rows:
        if d_id.isdigit():
            member = ctx.guild.get_member(int(d_id))
            if member:
                lines.append(f"• {member.mention}")
            else:
                # remove from DB
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (d_id,))
                conn.commit()
                removed_count += 1
        else:
            # also remove
            cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (d_id,))
            conn.commit()
            removed_count += 1

    if not lines:
        embed = discord.Embed(
            title="Unregistered Users",
            description=f"All fallback users have left; removed {removed_count} records.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    def chunk_string(txt, limit=1800):
        return [txt[i : i + limit] for i in range(0, len(txt), limit)]

    big_str = "\n".join(lines)
    chunks = chunk_string(big_str)
    for i, chunk in enumerate(chunks, start=1):
        embed = discord.Embed(
            title=f"Fallback Users (Page {i}/{len(chunks)})",
            description=chunk,
            color=discord.Color.orange()
        )
        if removed_count > 0 and i == 1:
            embed.set_footer(text=f"Removed {removed_count} DB entries for ex-members.")
        await ctx.send(embed=embed)

@bot.command(name="purge_database")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def purge_database(ctx):
    await ctx.send(
        "⚠️ **Are you sure you want to purge the database?**\n"
        "Type confirm to proceed or cancel to abort."
    )

    def check_confirm(m):
        return (
            m.author == ctx.author
            and m.channel == ctx.channel
            and m.content.lower() in ["confirm", "cancel"]
        )

    try:
        response = await bot.wait_for("message", timeout=30.0, check=check_confirm)
        if response.content.lower() == "confirm":
            cursor.execute("DELETE FROM Users")
            conn.commit()
            await ctx.send("The database has been purged. All records are deleted.")
        else:
            await ctx.send("Purge operation cancelled.")
    except asyncio.TimeoutError:
        await ctx.send("No response received. Operation cancelled.")

@bot.command(name="test")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def test(ctx):
    await ctx.send("The bot is working!")

@bot.command(name="update_ranks")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def update_ranks(ctx):
    """
    Go through the DB:
      - If the user is still in the server, update:
          Rank => highest qualifying role
          r_user => their current Discord display name
      - If the user left or DiscordID not numeric, remove them from the DB.
    """
    cursor.execute("SELECT DiscordID FROM Users")
    all_users = cursor.fetchall()

    updated_count = 0
    removed_count = 0
    await ctx.guild.chunk()

    for (disc_id_str,) in all_users:
        if disc_id_str.isdigit():
            member = ctx.guild.get_member(int(disc_id_str))
            if member:
                rank = get_highest_qualifying_role(member, ctx.guild) or "Unknown"
                # Set r_user to the member's current display name
                new_r_user = member.display_name
                cursor.execute(
                    "UPDATE Users SET Rank=%s, r_user=%s WHERE DiscordID=%s",
                    (rank, new_r_user, disc_id_str)
                )
                updated_count += 1
            else:
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
                removed_count += 1
        else:
            cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
            removed_count += 1

    conn.commit()
    await ctx.send(
        f"✅ Updated rank + display names for {updated_count} users. "
        f"Removed {removed_count} who left or had invalid IDs."
    )

# --------------------------------------------------------------------
# New Commands: !enforce_quota and !check_failed
# --------------------------------------------------------------------

@bot.command(name="enforce_quota")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def enforce_quota(ctx):
    """
    1) Recalculates quota
    2) Finds all users who have NOT met the quota, are NOT on inactivity,
       and do NOT have an exempt role (in exempt_role_ids).
    3) Adds 1 Strike to each of those users.
    4) Sends a message listing all members who received a strike.
       - Removes from DB any user who left the server or has invalid DiscordID.
    """
    try:
        recalculate_quota()
    except Exception as e:
        await ctx.send(f"❌ Error recalculating quota: {str(e)}")
        return

    try:
        cursor.execute(
            """
            SELECT DiscordID
            FROM Users
            WHERE QuotaMet = FALSE
              AND Inactive = FALSE
            """
        )
        rows = cursor.fetchall()
    except Exception as e:
        await ctx.send(f"❌ Error querying database: {str(e)}")
        return

    striked_members = []
    removed_count = 0
    error_count = 0

    try:
        await ctx.guild.chunk()
    except Exception as e:
        print(f"Warning: Could not chunk guild: {e}")
        # Continue anyway, chunking is not critical

    for (disc_id_str,) in rows:
        try:
            # Skip if not numeric
            if not disc_id_str.isdigit():
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
                conn.commit()
                removed_count += 1
                continue

            member = ctx.guild.get_member(int(disc_id_str))
            if not member:
                # user left => remove from DB
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
                conn.commit()
                removed_count += 1
                continue

            # Check exempt roles
            if any(role.id in exempt_role_ids for role in member.roles):
                continue

            # Add 1 strike
            cursor.execute("UPDATE Users SET Strikes = Strikes + 1 WHERE DiscordID=%s", (disc_id_str,))
            conn.commit()
            striked_members.append(member.mention)
        except Exception as e:
            error_count += 1
            print(f"Error processing user {disc_id_str}: {e}")
            continue

    # If no one was striked, send a simple message
    if not striked_members:
        msg = "No users were striked (either no one failed quota or all failing were exempt/inactive)."
        if removed_count > 0:
            msg += f"\nRemoved {removed_count} user(s) who left or had invalid IDs."
        if error_count > 0:
            msg += f"\n⚠️ {error_count} error(s) occurred while processing users."
        try:
            await ctx.send(msg)
        except Exception as e:
            await ctx.send(f"❌ Error sending message: {str(e)}")
        return

    # Otherwise, build the embed and chunk the striked list so we don't exceed 1024 chars in one field
    try:
        embed = discord.Embed(
            title="Enforce Quota",
            description="The following users have been given **1 Strike** for failing to meet quota:",
            color=discord.Color.red()
        )

        # Helper function: chunk a big list into lines that fit within 1024 chars
        def chunk_lines(lines, max_length=1024):
            """
            Takes a list of strings (lines) and yields combined
            strings that do not exceed max_length in total.
            """
            current_chunk = ""
            for line in lines:
                # +1 for newline
                if len(current_chunk) + len(line) + 1 > max_length:
                    yield current_chunk
                    current_chunk = line
                else:
                    if not current_chunk:
                        current_chunk = line
                    else:
                        current_chunk += "\n" + line
            if current_chunk:
                yield current_chunk

        # Chunk the striked list (each mention is a line)
        chunks = list(chunk_lines(striked_members))
        # Add each chunk as its own field in the embed
        for i, chunk in enumerate(chunks, start=1):
            embed.add_field(name=f"Striked (Part {i})", value=chunk, inline=False)

        footer_parts = []
        if removed_count > 0:
            footer_parts.append(f"Also removed {removed_count} user(s) who left or had invalid IDs.")
        if error_count > 0:
            footer_parts.append(f"{error_count} error(s) occurred while processing users.")
        
        if footer_parts:
            embed.set_footer(text=" | ".join(footer_parts))

        await ctx.send(embed=embed)
    except Exception as e:
        # Fallback to simple message if embed fails
        try:
            msg = f"✅ **Enforce Quota Complete**\n"
            msg += f"**{len(striked_members)} user(s) striked:**\n"
            msg += ", ".join(striked_members[:20])  # Limit to first 20 to avoid message length issues
            if len(striked_members) > 20:
                msg += f"\n... and {len(striked_members) - 20} more."
            if removed_count > 0:
                msg += f"\nRemoved {removed_count} user(s) who left or had invalid IDs."
            if error_count > 0:
                msg += f"\n⚠️ {error_count} error(s) occurred while processing users."
            await ctx.send(msg)
        except Exception as e2:
            await ctx.send(f"❌ Critical error: Could not send results. {str(e)} | {str(e2)}")
    


@bot.command(name="reset_strikes")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def reset_strikes(ctx):
    """
    Usage: !reset_strikes
    Resets EVERYONE's strikes in the DB to 0.
    """
    cursor.execute(
        """
        UPDATE Users
        SET Strikes=0
        """
    )
    conn.commit()
    await ctx.send("✅ All users' strikes have been reset to 0.")


@bot.command(name="check_failed")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def check_failed(ctx):
    """
    Shows all users who have 2 or more strikes OR have had the immune role for more than 2 weeks.
    Removes from DB those who left or have invalid ID.
    """
    cursor.execute("""
        SELECT DiscordID, Strikes, ImmuneRoleStart 
        FROM Users 
        WHERE Strikes >= 2 
           OR (ImmuneRoleStart IS NOT NULL AND ImmuneRoleStart <= CURRENT_DATE - INTERVAL '14 days')
    """)
    rows = cursor.fetchall()

    if not rows:
        await ctx.send("No users currently have 2 or more strikes or immune role for 2+ weeks.")
        return

    lines = []
    removed_count = 0
    await ctx.guild.chunk()

    for (disc_id_str, strikes, immune_start) in rows:
        if not disc_id_str.isdigit():
            # remove
            cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
            conn.commit()
            removed_count += 1
            continue

        member = ctx.guild.get_member(int(disc_id_str))
        if not member:
            # remove
            cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
            conn.commit()
            removed_count += 1
            continue

        if strikes >= 2:
            lines.append(f"• {member.mention} has **{strikes}** strike(s).")
        elif immune_start:
            lines.append(f"• {member.mention} has had immune role for **2+ weeks** (since {immune_start}).")

    if not lines:
        msg = "No valid users with 2+ strikes or immune role for 2+ weeks in the database."
        if removed_count > 0:
            msg += f" Removed {removed_count} invalid entries."
        await ctx.send(msg)
        return

    def chunk_string(txt, limit=1800):
        return [txt[i : i + limit] for i in range(0, len(txt), limit)]

    big_str = "\n".join(lines)
    chunks = chunk_string(big_str)
    for i, chunk in enumerate(chunks, 1):
        embed = discord.Embed(
            title=f"Users with 2+ Strikes or 2+ Week Immune Role (Page {i}/{len(chunks)})",
            description=chunk,
            color=discord.Color.red()
        )
        if removed_count > 0 and i == 1:
            embed.set_footer(text=f"Removed {removed_count} invalid user(s).")
        await ctx.send(embed=embed)

# --------------------------------------------------------------------
# 1) Everyone can use !commands
# --------------------------------------------------------------------
@bot.command(name="commands")
async def list_commands(ctx):
    """
    Displays all available commands that the user has access to.
    """
    accessible_commands = []
    for command in bot.commands:
        try:
            if await command.can_run(ctx):
                accessible_commands.append(command.name)
        except commands.CheckFailure:
            continue

    commands_str = "\n".join(f"!{name}" for name in accessible_commands)
    embed = discord.Embed(
        title="Available Commands",
        description=commands_str,
        color=discord.Color.blue()
    )
    await ctx.send(embed=embed)

# --------------------------------------------------------------------
# 2) Everyone can use !lookup
# --------------------------------------------------------------------
@bot.command(name="lookup")
async def lookup(ctx, user: discord.Member):
    """
    Usage: !lookup @someuser
    Displays that user's info if present in the DB (no role restriction).
    """
    disc_id_str = str(user.id)
    cursor.execute(
        """
        SELECT RobloxID, r_user, EventsAttended, EventsHosted, FlightMinutes, QuotaMet, Strikes
        FROM Users
        WHERE DiscordID=%s
        """,
        (disc_id_str,)
    )
    row = cursor.fetchone()

    if not row:
        await ctx.send(f"No database entry found for {user.mention}.")
        return

    roblox_id, r_user_val, attended, hosted, flight_mins, quota_met, strikes = row
    attended = attended or 0
    hosted = hosted or 0
    flight_mins = flight_mins or 0
    quota_str = "Yes" if quota_met else "No"

    embed = discord.Embed(
        title=f"Lookup for {user.display_name}",
        color=discord.Color.blue()
    )
    embed.add_field(name="Discord ID", value=disc_id_str, inline=True)
    embed.add_field(name="Roblox ID", value=roblox_id or "None", inline=True)
    embed.add_field(name="r_user", value=r_user_val or "None", inline=True)
    embed.add_field(name="Events Attended", value=str(attended), inline=True)
    embed.add_field(name="Events Hosted", value=str(hosted), inline=True)
    embed.add_field(name="Flight Minutes", value=str(flight_mins), inline=True)
    embed.add_field(name="Quota Complete", value=quota_str, inline=True)
    embed.add_field(name="Strikes", value=str(strikes or 0), inline=True)

    await ctx.send(embed=embed)

# --------------------------------------------------------------------
# 3) !wipe_user (Resets one user's counters)
# --------------------------------------------------------------------
@bot.command(name="wipe_user")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def wipe_user(ctx, user: discord.Member):
    """
    Usage: !wipe_user @Member
    Resets a single user's counters: EventsAttended=0, EventsHosted=0,
    FlightMinutes=0, QuotaMet=FALSE, Strikes=0.
    Keeps RobloxID, r_user, Rank, etc. intact.
    """
    disc_id_str = str(user.id)
    # Check if user exists in DB
    cursor.execute("SELECT DiscordID FROM Users WHERE DiscordID=%s", (disc_id_str,))
    row = cursor.fetchone()

    if not row:
        await ctx.send(f"No database record found for {user.mention}.")
        return

    cursor.execute(
        """
        UPDATE Users
        SET EventsAttended=0,
            EventsHosted=0,
            FlightMinutes=0,
            QuotaMet=FALSE,
            Strikes=0
        WHERE DiscordID=%s
        """,
        (disc_id_str,)
    )
    conn.commit()
    await ctx.send(f"✅ {user.mention}'s counters have been reset to 0 and QuotaMet set to False.")

# --------------------------------------------------------------------
# 4) !reset_quota (Resets everyone's counters)
# --------------------------------------------------------------------
@bot.command(name="reset_quota")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def reset_quota_command(ctx):
    """
    Usage: !reset_quota
    Resets EVERYONE's counters in the DB:
    - EventsAttended = 0
    - EventsHosted = 0
    - FlightMinutes = 0
    - QuotaMet = FALSE
    """
    cursor.execute(
        """
        UPDATE Users
        SET EventsAttended=0,
            EventsHosted=0,
            FlightMinutes=0,
            QuotaMet=FALSE
        """
    )
    conn.commit()
    await ctx.send("✅ All users' counters have been reset to 0, and QuotaMet set to False.")

# --------------------------------------------------------------------
# NEW COMMAND: !request_inactivity
# --------------------------------------------------------------------
@bot.command(name="request_inactivity")
async def request_inactivity(ctx):
    """
    A multi-step command to request inactivity:
      1) Asks user for start date (YYYY-MM-DD).
      2) Asks user for end date (YYYY-MM-DD).
      3) Asks user for reason.
      4) Posts an embed in the inactivity approval channel for staff to approve/deny.
    """
    def check_author(m):
        return (m.author == ctx.author and m.channel == ctx.channel)

    # Step 1: Ask for start date
    await ctx.send("📅 Please provide your **start date** for inactivity (YYYY-MM-DD). You have 60s.")
    try:
        start_msg = await bot.wait_for("message", timeout=60.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long. Inactivity request cancelled.")
        return

    start_date_str = start_msg.content.strip()
    # Validate format
    try:
        time.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        await ctx.send("❌ Invalid date format. Use YYYY-MM-DD. Request cancelled.")
        return

    # Step 2: Ask for end date
    await ctx.send("📅 Please provide your **end date** for inactivity (YYYY-MM-DD). You have 60s.")
    try:
        end_msg = await bot.wait_for("message", timeout=60.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long. Inactivity request cancelled.")
        return

    end_date_str = end_msg.content.strip()
    try:
        time.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        await ctx.send("❌ Invalid date format. Use YYYY-MM-DD. Request cancelled.")
        return

    # Step 3: Reason
    await ctx.send("✏️ Please provide the **reason** for your inactivity. You have 120s.")
    try:
        reason_msg = await bot.wait_for("message", timeout=120.0, check=check_author)
    except asyncio.TimeoutError:
        await ctx.send("❌ You took too long. Inactivity request cancelled.")
        return

    reason = reason_msg.content.strip()
    if not reason:
        await ctx.send("❌ No reason provided. Request cancelled.")
        return

    # Step 4: Post to inactivity approval channel
    inactivity_channel = bot.get_channel(INACTIVITY_APPROVAL_CHANNEL_ID)
    if not inactivity_channel:
        await ctx.send("❌ Could not find the inactivity approval channel. Contact an admin.")
        return

    embed = discord.Embed(
        title="Inactivity Request",
        color=discord.Color.orange(),
        description=(
            f"**User:** {ctx.author.mention}\n"
            f"**Start Date:** {start_date_str}\n"
            f"**End Date:** {end_date_str}\n"
            f"**Reason:** {reason}\n"
        )
    )
    embed.set_footer(text="React with ✅ to approve or ❌ to deny.")

    approval_message = await inactivity_channel.send(embed=embed)
    await approval_message.add_reaction("✅")
    await approval_message.add_reaction("❌")

    pending_inactivity_requests[approval_message.id] = {
        "user_id": ctx.author.id,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "reason": reason
    }

    await ctx.send("✅ Your inactivity request has been submitted for approval.")

@bot.command(name="end_inactivity")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def end_inactivity(ctx, user: discord.Member):
    """
    Usage: !end_inactivity @Member
    Sets the user as active again in the DB, clearing inactivity fields.
    """
    disc_id_str = str(user.id)
    cursor.execute("SELECT DiscordID FROM Users WHERE DiscordID=%s", (disc_id_str,))
    row = cursor.fetchone()

    if not row:
        await ctx.send(f"No database record found for {user.mention}.")
        return

    cursor.execute(
        """
        UPDATE Users
        SET Inactive=FALSE,
            InactiveStart=NULL,
            InactiveEnd=NULL,
            InactiveReason=NULL
        WHERE DiscordID=%s
        """,
        (disc_id_str,)
    )
    conn.commit()

    await ctx.send(f"✅ {user.mention} is now marked as active.")

@bot.command(name="display_inactivity")
@require_specific_role(REQUIRED_ROLE_ID_FOR_OTHERS)
async def display_inactivity(ctx):
    """
    Displays all users who are currently marked as inactive, along with their start date, end date, and reason.
    """
    cursor.execute(
        """
        SELECT DiscordID, InactiveStart, InactiveEnd, InactiveReason
        FROM Users
        WHERE Inactive = TRUE
        """
    )
    rows = cursor.fetchall()

    if not rows:
        embed = discord.Embed(
            title="Inactivity Report",
            description="No users are currently marked as inactive.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    lines = []
    for disc_id_str, start_date, end_date, reason in rows:
        if disc_id_str.isdigit():
            member = ctx.guild.get_member(int(disc_id_str))
            if member:
                lines.append(
                    f"• {member.mention}\n"
                    f"  **Start Date:** {start_date}\n"
                    f"  **End Date:** {end_date}\n"
                    f"  **Reason:** {reason}\n"
                )
            else:
                # user left the server, remove from DB
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id_str,))
                conn.commit()

    # After cleanup, re-check
    if not lines:
        embed = discord.Embed(
            title="Inactivity Report",
            description="No users are currently marked as inactive.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    def chunk_string(txt, limit=1800):
        return [txt[i : i + limit] for i in range(0, len(txt), limit)]

    big_text = "\n".join(lines)
    chunks = chunk_string(big_text)
    for i, chunk in enumerate(chunks, start=1):
        embed = discord.Embed(
            title=f"Inactivity Report (Page {i}/{len(chunks)})",
            description=chunk,
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed)

# --------------------------------------------------------------------
# EVENTS
# --------------------------------------------------------------------
@bot.event
async def on_ready():
    print(f"Bot is ready! Logged in as {bot.user}")

@bot.event
async def on_reaction_add(reaction, user):
    # Ignore bots
    if user.bot:
        return

    # We allow flight log approvals in REVIEW_CHANNEL_ID
    # and inactivity requests in INACTIVITY_APPROVAL_CHANNEL_ID
    if reaction.message.channel.id not in [REVIEW_CHANNEL_ID, INACTIVITY_APPROVAL_CHANNEL_ID]:
        return

    msg_id = reaction.message.id

    # 1) Check if it's a flight log
    if msg_id in pending_flight_logs:
        flight_log = pending_flight_logs[msg_id]
        flight_user_id = flight_log["user_id"]
        minutes = flight_log["minutes"]
        kills = flight_log["kills"]
        origin_channel_id = flight_log["origin_channel_id"]

        if str(reaction.emoji) not in ["✅", "❌"]:
            return

        origin_channel = bot.get_channel(origin_channel_id)

        if str(reaction.emoji) == "✅":
            # Approved flight log
            disc_id = str(flight_user_id)
            member = reaction.message.guild.get_member(flight_user_id)
            if member:
                ensure_user_record(member, reaction.message.guild)
                cursor.execute(
                    "UPDATE Users SET FlightMinutes = FlightMinutes + %s, weeklykills = weeklykills + %s WHERE DiscordID = %s",
                    (minutes,  kills, disc_id)
                )
                conn.commit()
                recalculate_quota()
                if origin_channel:
                    await origin_channel.send(
                        f"✅ <@{flight_user_id}>, your flight log has been approved "
                        f"and {minutes} minutes alongside {kills} kills have been added to your record."
                    )
            else:
                # user left => remove from DB
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id,))
                conn.commit()
                if origin_channel:
                    await origin_channel.send(
                        f"❌ We could not find that user in the server; removed them from the DB."
                    )

        elif str(reaction.emoji) == "❌":
            # Denied flight log
            if origin_channel:
                await origin_channel.send(f"❌ <@{flight_user_id}>, your flight log has been denied.")

        # Remove from the pending logs
        del pending_flight_logs[msg_id]

    # 2) Check if it's an inactivity request
    elif msg_id in pending_inactivity_requests:
        if str(reaction.emoji) not in ["✅", "❌"]:
            return

        inactivity_request = pending_inactivity_requests[msg_id]
        request_user_id = inactivity_request["user_id"]
        start_date_str = inactivity_request["start_date"]
        end_date_str = inactivity_request["end_date"]
        reason = inactivity_request["reason"]

        if str(reaction.emoji) == "✅":
            # Approved => set the user as inactive in DB
            disc_id = str(request_user_id)
            member = reaction.message.guild.get_member(request_user_id)
            if member:
                ensure_user_record(member, reaction.message.guild)
                cursor.execute(
                    """
                    UPDATE Users
                    SET Inactive = TRUE,
                        InactiveStart = %s,
                        InactiveEnd = %s,
                        InactiveReason = %s
                    WHERE DiscordID = %s
                    """,
                    (start_date_str, end_date_str, reason, disc_id)
                )
                conn.commit()
                # Optionally DM the user
                try:
                    await member.send(
                        f"✅ Your inactivity request (from {start_date_str} to {end_date_str}) was approved."
                    )
                except discord.Forbidden:
                    pass
            else:
                # If user left, remove from DB
                cursor.execute("DELETE FROM Users WHERE DiscordID=%s", (disc_id,))
                conn.commit()
        else:
            # Denied => do nothing in DB
            member = reaction.message.guild.get_member(request_user_id)
            if member:
                try:
                    await member.send("❌ Your inactivity request has been denied.")
                except discord.Forbidden:
                    pass

        # Remove from pending requests
        del pending_inactivity_requests[msg_id]

# --------------------------------------------------------------------
# RUN THE BOT
# --------------------------------------------------------------------
bot.run(BOT_TOKEN)
